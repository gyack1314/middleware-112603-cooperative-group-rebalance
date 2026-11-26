"""消费组协调器核心：成员能力、分区分配、生效交接与消费进度。

- 分区内位置从 1 起；low 为最早保留点，high 为已发布最高位置。
- committed 为已连续确认到的位置；确认只承认实际投递过的位置，已领取不等于已处理。
- assignment 是分区实际所有权的代次，每次实际接管递增，用于栅栏过期操作。
- 交接（handoff）记录实际投递边界，再平衡与重启都不会遗忘。
- 重新协调是惰性的：成员或分区变化先登记，rebalance 才重算归属；
  交接完成（确认完毕 / 明确交还 / 租约过期 / 管理员重置）随时生效。
- 只依赖标准库，状态可整体序列化，重启后未决交接与分区进度原样恢复。
"""

from __future__ import annotations

import bisect
from dataclasses import asdict, dataclass, field

DEFAULT_DRAIN_TTL = 30  # 旧租期长度（与演练记录相同的整数秒点）
EVENT_LOG_LIMIT = 100
HISTORY_LIMIT = 200


def _ok(**fields):
    return {"ok": True, **fields}


def _err(code, detail="", **fields):
    return {"ok": False, "error": {"code": code, "detail": detail, **fields}}


@dataclass
class Member:
    id: str
    boot: int
    formats: list
    slots: int

    @classmethod
    def from_dict(cls, data):
        return cls(id=data["id"], boot=data["boot"],
                   formats=list(data["formats"]), slots=data["slots"])


@dataclass
class Handoff:
    """一次移交承诺。delivered_boundary 是旧成员实际投递边界，不可遗忘。"""

    handoff_id: int
    from_member: object
    from_assignment: int
    to_member: object
    to_assignment: int
    delivered_boundary: int
    created_at: int
    lease_deadline: int
    forced: bool = False
    state: str = "draining"          # draining | blocked（保留点越过未确认位置）
    giveback_received: bool = False
    lost_from: object = None         # blocked 时保留点越过的未确认范围
    lost_to: object = None

    @classmethod
    def from_dict(cls, data):
        return cls(**data)


@dataclass
class Partition:
    name: str
    format: str
    priority: int
    low: int
    high: int
    committed: int                   # 已连续确认到的位置
    owner: object = None             # 实际拥有者（交接期间仍是旧成员，直至完成）
    assignment: int = 0              # 实际所有权代次
    delivered: int = 0               # 当前代次实际投递到的位置
    next_handoff_id: int = 1
    pending: object = None           # 未决交接（Handoff）
    blocked: object = None           # 非交接中的保留点阻塞
    unassigned: object = None        # 留空原因
    note: str = "尚未分配"            # 最近一次协调决策（运营可读）
    events: list = field(default_factory=list)
    history: list = field(default_factory=list)  # 已完成交接与重置记录

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, data):
        data = dict(data)
        pending = data.pop("pending", None)
        partition = cls(**data)
        partition.pending = Handoff.from_dict(pending) if pending else None
        return partition


class Group:
    """一个消费组的全部状态与协调逻辑。"""

    def __init__(self, name, drain_ttl=DEFAULT_DRAIN_TTL):
        self.name = name
        self.drain_ttl = int(drain_ttl)
        self.members = {}
        self.partitions = {}

    # ---- 序列化 ---------------------------------------------------------

    def to_dict(self):
        return {
            "name": self.name,
            "drain_ttl": self.drain_ttl,
            "members": {mid: asdict(m) for mid, m in self.members.items()},
            "partitions": {p.name: p.to_dict() for p in self.partitions.values()},
        }

    @classmethod
    def from_dict(cls, data):
        group = cls(data["name"], data.get("drain_ttl", DEFAULT_DRAIN_TTL))
        group.members = {mid: Member.from_dict(m) for mid, m in data.get("members", {}).items()}
        group.partitions = {
            p["name"]: Partition.from_dict(p) for p in data.get("partitions", {}).values()
        }
        return group

    # ---- 内部小工具 -----------------------------------------------------

    def _event(self, partition, at, kind, **fields):
        partition.events.append({"at": at, "kind": kind, **fields})
        if len(partition.events) > EVENT_LOG_LIMIT:
            del partition.events[: len(partition.events) - EVENT_LOG_LIMIT]

    def _record_history(self, partition, record):
        partition.history.append(record)
        if len(partition.history) > HISTORY_LIMIT:
            del partition.history[: len(partition.history) - HISTORY_LIMIT]

    # ---- 对外操作 -------------------------------------------------------

    def declare_partition(self, at, name, format, priority, low, high):
        """发布或更新分区。格式不可变；保留点只能前进；优先级可提升。"""
        self._expire_handoffs(at)
        if not (isinstance(low, int) and isinstance(high, int)) or low < 1 or high < low - 1:
            return _err("invalid_span", f"需要 1 <= low 且 high >= low - 1，收到 low={low} high={high}")
        if not isinstance(priority, int):
            return _err("invalid_priority", "priority 应为整数")
        partition = self.partitions.get(name)
        created = partition is None
        if created:
            partition = Partition(name=name, format=format, priority=priority,
                                  low=low, high=high, committed=low - 1, delivered=low - 1)
            partition.note = "已发布，等待 rebalance 分配"
            self.partitions[name] = partition
            self._event(partition, at, "declared", format=format, priority=priority,
                        low=low, high=high)
        else:
            if format != partition.format:
                return _err("format_immutable",
                            f"分区 {name} 的格式 {partition.format} 不可变更为 {format}")
            if low < partition.low:
                return _err("retention_rewind",
                            f"保留点不能倒退：当前 {partition.low}，收到 {low}")
            changes = []
            for key, new in (("priority", priority), ("low", low), ("high", high)):
                old = getattr(partition, key)
                if old != new:
                    changes.append(f"{key} {old}->{new}")
            partition.priority, partition.low, partition.high = priority, low, high
            self._event(partition, at, "updated", changes=changes)
            self._check_trim_block(partition, at)
        return _ok(partition=name, created=created,
                   state=self._partition_state(partition),
                   note="已登记；归属将在下次 rebalance 重算")

    def upsert_member(self, at, member_id, boot, formats, slots):
        """成员上线 / 更新（formats、slots）/ 退出（slots=0）。boot 变化视为新化身。"""
        self._expire_handoffs(at)
        if not isinstance(slots, int) or slots < 0:
            return _err("invalid_slots", "slots 应为非负整数，0 表示退出")
        formats = list(formats)
        existing = self.members.get(member_id)
        if slots == 0:
            if existing is None:
                return _ok(member=member_id, note="成员本就不在组中")
            del self.members[member_id]
            return _ok(member=member_id, note="成员已退出；其分区将在下次 rebalance 重算")
        if existing is not None and existing.boot != boot:
            # 新化身：旧化身持有的分区立即失效，在途交付作废，从已确认位置重投。
            del self.members[member_id]
            for partition in self.partitions.values():
                if partition.pending is None and partition.owner == member_id:
                    self._event(partition, at, "owner_reincarnated",
                                member=member_id, boot=boot)
                    partition.owner = None
                    partition.delivered = partition.committed
                    partition.note = "成员化身更替，等待 rebalance 重算"
            existing = None
        self.members[member_id] = Member(id=member_id, boot=boot, formats=formats, slots=slots)
        action = "更新" if existing else "上线"
        return _ok(member=member_id, note=f"成员已{action}；归属将在下次 rebalance 重算")

    def rebalance(self, at, force_move=None):
        """重新协调。只挪动必要的分区，其余继续投递。"""
        self._expire_handoffs(at)
        decisions = self._recompute(at, force_move=force_move)
        return _ok(decisions=decisions)

    def force_revoke(self, at, partition):
        """管理员入口：强制撤回某分区当前归属，交给其它合格成员（或留空）。"""
        self._expire_handoffs(at)
        target = self.partitions.get(partition)
        if target is None:
            return _err("unknown_partition", f"分区 {partition} 不存在")
        if target.pending is not None:
            return _err("handoff_already_pending",
                        f"交接 #{target.pending.handoff_id} 进行中",
                        handoff=target.pending.handoff_id)
        if target.owner is None:
            return _err("not_owned", "分区当前无人持有，无需撤回")
        decisions = self._recompute(at, force_move={partition: target.owner})
        decision = next((d for d in decisions if d["partition"] == partition), None)
        handoff = target.pending
        if decision is not None and decision["decision"] == "revoked":
            if handoff is not None:
                return _ok(partition=partition, handoff=handoff.handoff_id,
                           from_member=handoff.from_member,
                           to_member=handoff.to_member,
                           delivered_boundary=handoff.delivered_boundary,
                           decisions=decisions)
            return _ok(partition=partition, owner=target.owner,
                       assignment=target.assignment,
                       note="撤回生效：旧成员无在途批次，交接当场完成",
                       decisions=decisions)
        return _ok(partition=partition, owner=target.owner,
                   note="撤回未生效：没有其它合格成员，原成员继续持有",
                   decisions=decisions)

    def poll(self, at, member_id, partition, assignment, limit=1):
        """领取。每次最多取 limit 条（演练记录约定为 1）。"""
        self._expire_handoffs(at)
        target = self.partitions.get(partition)
        if target is None:
            return _err("unknown_partition", f"分区 {partition} 不存在")
        handoff = target.pending
        if handoff is not None:
            if member_id == handoff.from_member and assignment == handoff.from_assignment:
                return _err("revoked",
                            f"分区已撤回（交接 #{handoff.handoff_id}），停止新批次；"
                            f"可继续确认已取批次（边界 {handoff.delivered_boundary}）或明确交还",
                            handoff=handoff.handoff_id)
            if member_id == handoff.to_member and assignment == handoff.to_assignment:
                return _err("handoff_pending",
                            f"交接 #{handoff.handoff_id} 未完成：{self._waiting_on(target)}",
                            handoff=handoff.handoff_id)
        if target.owner != member_id:
            return _err("not_owner", f"分区 {partition} 当前由 {target.owner} 持有",
                        owner=target.owner, assignment=target.assignment)
        if assignment != target.assignment:
            return _err("stale_assignment",
                        f"assignment 代次不符：当前 {target.assignment}，收到 {assignment}",
                        current_assignment=target.assignment)
        if member_id not in self.members:
            return _err("owner_gone", "持有者已不在组中，等待 rebalance")
        if target.blocked is not None:
            return _err("blocked", self._waiting_on(target), blocked=target.blocked)
        messages = []
        while len(messages) < limit and target.delivered < target.high:
            nxt = target.delivered + 1
            if nxt < target.low:
                target.blocked = {
                    "lost_from": target.committed + 1, "lost_to": target.low - 1,
                    "token_assignment": target.assignment,
                    "detail": "保留点越过待投位置，组不能自行跳过，等待管理员 reset",
                }
                self._event(target, at, "delivery_blocked",
                            lost_from=target.committed + 1, lost_to=target.low - 1)
                return _err("blocked", target.blocked["detail"], blocked=target.blocked)
            target.delivered = nxt
            messages.append({"partition": target.name, "position": nxt,
                             "format": target.format,
                             "payload": f"msg-{target.name}-{nxt}",
                             "assignment": target.assignment})
        return _ok(partition=target.name, member=member_id, assignment=target.assignment,
                   messages=messages, committed=target.committed,
                   delivered=target.delivered, low=target.low, high=target.high)

    def ack(self, at, member_id, partition, assignment, through):
        """确认。必须连续到已实际完成的位置，且不得超过实际投递边界。"""
        self._expire_handoffs(at)
        target = self.partitions.get(partition)
        if target is None:
            return _err("unknown_partition", f"分区 {partition} 不存在")
        if not isinstance(through, int) or through < 0:
            return _err("invalid_through", "through 应为非负整数")
        handoff = target.pending
        if handoff is not None:
            if member_id == handoff.from_member and assignment == handoff.from_assignment:
                return self._ack_draining(at, target, handoff, through)
            if member_id == handoff.to_member and assignment == handoff.to_assignment:
                return _err("handoff_pending",
                            f"交接 #{handoff.handoff_id} 未完成，新成员尚无可确认位置")
        if target.owner == member_id and assignment == target.assignment:
            if through > target.delivered:
                return _err("beyond_delivered",
                            f"确认位置 {through} 越过实际投递 {target.delivered}，"
                            "不能把已领取当已处理", delivered=target.delivered)
            if through <= target.committed:
                return _ok(partition=partition, committed=target.committed,
                           note="重复确认，无推进")
            target.committed = through
            return _ok(partition=partition, committed=target.committed)
        # 其余情况：过期代次的迟到确认。不得越过新成员已取得的批次，整段拒绝。
        completed = self._find_handoff_record(target, member_id, assignment)
        self._event(target, at, "late_ack_rejected", member=member_id,
                    assignment=assignment, through=through,
                    current_assignment=target.assignment,
                    handoff_completed_by=completed)
        return _err("stale_assignment",
                    f"旧租期的迟到确认被拒（当前 assignment {target.assignment}）："
                    "不得越过新成员已取得的批次",
                    current_owner=target.owner, current_assignment=target.assignment,
                    handoff_completed_by=completed)

    def giveback(self, at, member_id, partition, assignment):
        """明确交还：旧成员放弃已领取但未处理的部分，交接立即生效。"""
        self._expire_handoffs(at)
        target = self.partitions.get(partition)
        if target is None:
            return _err("unknown_partition", f"分区 {partition} 不存在")
        handoff = target.pending
        if handoff is None:
            return _ok(partition=partition, committed=target.committed,
                   note="无待交接，giveback 为幂等空操作")
        if member_id != handoff.from_member or assignment != handoff.from_assignment:
            return _err("not_handoff_owner",
                        f"交接 #{handoff.handoff_id} 的交还只能由 "
                        f"{handoff.from_member} 以 assignment {handoff.from_assignment} 发起")
        handoff.giveback_received = True
        self._event(target, at, "giveback", handoff=handoff.handoff_id, member=member_id,
                    returned_from=target.committed + 1, returned_to=handoff.delivered_boundary)
        if handoff.state == "blocked":
            return _ok(partition=partition, handoff=handoff.handoff_id,
                   note="已登记交还；但保留点越过未确认位置，仍等待管理员 reset",
                   lost={"from": handoff.lost_from, "to": handoff.lost_to})
        self._complete_handoff(target, at, "giveback")
        return _ok(partition=partition, handoff=handoff.handoff_id, completed="giveback",
                   committed=target.committed, owner=target.owner,
                   assignment=target.assignment)

    def reset(self, at, partition, handoff_id, loss_from, loss_to):
        """管理员授权：按明确损失范围重置，并绑定本次移交身份。"""
        self._expire_handoffs(at)
        target = self.partitions.get(partition)
        if target is None:
            return _err("unknown_partition", f"分区 {partition} 不存在")
        handoff = target.pending
        if handoff is not None:
            if handoff.state != "blocked":
                return _err("handoff_not_blocked",
                            f"交接 #{handoff.handoff_id} 未被保留点阻塞，无需 reset")
            if handoff_id != handoff.handoff_id:
                return _err("handoff_mismatch",
                            f"reset 必须绑定本次移交身份：期望 handoff={handoff.handoff_id}，"
                            f"收到 {handoff_id}", expected=handoff.handoff_id)
            expected = {"from": target.committed + 1, "to": target.low - 1}
            if loss_from != expected["from"] or loss_to != expected["to"]:
                return _err("loss_range_mismatch",
                            f"损失范围必须精确覆盖未确认缺口 {expected}，"
                            f"收到 [{loss_from},{loss_to}]", expected=expected)
            target.committed = loss_to
            self._event(target, at, "reset", handoff=handoff.handoff_id,
                        loss_from=loss_from, loss_to=loss_to)
            self._complete_handoff(target, at, "reset")
            return _ok(partition=partition, handoff=handoff.handoff_id,
                       committed=target.committed, owner=target.owner,
                       assignment=target.assignment,
                       authorized_loss={"from": loss_from, "to": loss_to})
        if target.blocked is not None:
            token = target.blocked["token_assignment"]
            if handoff_id != token:
                return _err("handoff_mismatch",
                            f"非交接阻塞以当前 assignment 为核对标识：期望 {token}，"
                            f"收到 {handoff_id}", expected=token)
            expected = {"from": target.committed + 1, "to": target.low - 1}
            if loss_from != expected["from"] or loss_to != expected["to"]:
                return _err("loss_range_mismatch",
                            f"损失范围必须精确覆盖未确认缺口 {expected}，"
                            f"收到 [{loss_from},{loss_to}]", expected=expected)
            target.committed = loss_to
            target.delivered = max(target.delivered, target.committed)
            target.blocked = None
            self._event(target, at, "reset", handoff=None,
                        loss_from=loss_from, loss_to=loss_to)
            return _ok(partition=partition, committed=target.committed,
                       authorized_loss={"from": loss_from, "to": loss_to})
        return _err("no_blocked_handoff", "分区没有被保留点阻塞的交接，无需 reset")

    # ---- 运营视图 -------------------------------------------------------

    def inspect(self, partition):
        target = self.partitions.get(partition)
        if target is None:
            return _err("unknown_partition", f"分区 {partition} 不存在")
        return _ok(
            partition=target.name, format=target.format, priority=target.priority,
            low=target.low, high=target.high,
            committed=target.committed, next_position=target.committed + 1,
            owner=target.owner, assignment=target.assignment, delivered=target.delivered,
            state=self._partition_state(target),
            waiting_on=self._waiting_on(target),
            note=target.note,
            pending_handoff=asdict(target.pending) if target.pending else None,
            blocked=target.blocked,
            unassigned=target.unassigned,
            events=list(target.events[-10:]),
            history=list(target.history[-10:]),
        )

    def status(self):
        return _ok(
            group=self.name, drain_ttl=self.drain_ttl,
            members=[asdict(m) for m in sorted(self.members.values(), key=lambda m: m.id)],
            partitions=[self.inspect(name) for name in sorted(self.partitions)],
            pending_handoffs=[
                {"partition": p.name, **asdict(p.pending)}
                for p in sorted(self.partitions.values(), key=lambda x: x.name)
                if p.pending is not None
            ],
        )

    def handoffs(self, completed_by=None):
        """交接台账。completed_by 可分别查 expired / giveback / drained / reset / rescinded。"""
        done = []
        for partition in self.partitions.values():
            for record in partition.history:
                if completed_by is None or record["completed_by"] == completed_by:
                    done.append(dict(record))
        done.sort(key=lambda r: (r["completed_at"], r["partition"], r["handoff"]))
        pending = [
            {"partition": p.name, **asdict(p.pending)}
            for p in sorted(self.partitions.values(), key=lambda x: x.name)
            if p.pending is not None
        ]
        return _ok(pending=pending, completed=done)

    # ---- 内部：交接生命周期 ---------------------------------------------

    def _ack_draining(self, at, target, handoff, through):
        if through > handoff.delivered_boundary:
            return _err("beyond_delivered",
                        f"确认位置 {through} 越过实际投递边界 {handoff.delivered_boundary}",
                        boundary=handoff.delivered_boundary)
        if through <= target.committed:
            return _ok(partition=target.name, committed=target.committed,
                       note="重复确认，无推进")
        target.committed = through
        # 确认可能填平保留点越过的缺口：缺口闭合则解除阻塞（这不是跳过，是如实确认）。
        if handoff.state == "blocked" and target.committed + 1 >= target.low:
            handoff.state = "draining"
            handoff.lost_from = handoff.lost_to = None
            self._event(target, at, "handoff_unblocked", handoff=handoff.handoff_id)
        completed = None
        if handoff.state != "blocked" and target.committed >= handoff.delivered_boundary:
            completed = "drained"
            self._complete_handoff(target, at, "drained")
        return _ok(partition=target.name, committed=target.committed,
                   handoff_completed=completed, owner=target.owner,
                   assignment=target.assignment)

    def _complete_handoff(self, partition, at, how):
        handoff = partition.pending
        if handoff is None:
            return
        record = {
            "partition": partition.name,
            "handoff": handoff.handoff_id,
            "from_member": handoff.from_member,
            "to_member": handoff.to_member,
            "from_assignment": handoff.from_assignment,
            "to_assignment": handoff.to_assignment,
            "delivered_boundary": handoff.delivered_boundary,
            "giveback_received": handoff.giveback_received,
            "forced": handoff.forced,
            "completed_by": how,
            "completed_at": at,
            "committed_at_completion": partition.committed,
        }
        if how == "reset":
            record["authorized_loss"] = {"from": handoff.lost_from, "to": handoff.lost_to}
        self._record_history(partition, record)
        self._event(partition, at, "handoff_completed", handoff=handoff.handoff_id,
                    how=how, to_member=handoff.to_member, committed=partition.committed)
        partition.pending = None
        target = handoff.to_member
        member = self.members.get(target) if target else None
        if member is not None and member.slots > 0 and partition.format in member.formats:
            partition.owner = target
            partition.assignment = handoff.to_assignment
            partition.delivered = partition.committed
            partition.note = (f"交接 #{handoff.handoff_id} 完成（{how}）：{target} 以 "
                              f"assignment {partition.assignment} 接管，"
                              f"从位置 {partition.committed + 1} 起投递")
        else:
            partition.owner = None
            members = {m.id: m for m in self.members.values() if m.slots > 0}
            partition.unassigned = self._unassigned_reason(partition, members)
            partition.note = (f"交接 #{handoff.handoff_id} 完成（{how}），"
                              "但目标不可用或暂无合格成员，等待 rebalance")

    def _expire_handoffs(self, at):
        for partition in self.partitions.values():
            handoff = partition.pending
            if handoff is not None and handoff.state == "draining" \
                    and at > handoff.lease_deadline:
                self._event(partition, at, "lease_expired",
                            handoff=handoff.handoff_id, from_member=handoff.from_member)
                self._complete_handoff(partition, at, "expired")

    def _find_handoff_record(self, partition, member_id, assignment):
        for record in reversed(partition.history):
            if record["from_member"] == member_id and record["from_assignment"] == assignment:
                return record["completed_by"]
        return None

    # ---- 内部：分配计算 ---------------------------------------------------

    def _recompute(self, at, force_move=None):
        desired, reasons = self._compute_desired(force_move)
        decisions = []
        for name in sorted(self.partitions):
            partition = self.partitions[name]
            target = desired.get(name)
            handoff = partition.pending
            if handoff is not None:
                if target == handoff.from_member and not handoff.forced:
                    partition.pending = None
                    partition.note = (f"撤回撤销：{handoff.from_member} 仍合法且无需移动，"
                                      "继续投递")
                    self._event(partition, at, "handoff_rescinded", handoff=handoff.handoff_id)
                    self._record_history(partition, {
                        "partition": name, "handoff": handoff.handoff_id,
                        "from_member": handoff.from_member, "to_member": handoff.to_member,
                        "from_assignment": handoff.from_assignment,
                        "to_assignment": handoff.to_assignment,
                        "delivered_boundary": handoff.delivered_boundary,
                        "giveback_received": handoff.giveback_received,
                        "forced": handoff.forced,
                        "completed_by": "rescinded", "completed_at": at,
                        "committed_at_completion": partition.committed,
                    })
                    decisions.append({"partition": name, "decision": "handoff_rescinded",
                                      "handoff": handoff.handoff_id})
                elif target != handoff.to_member:
                    handoff.to_member = target
                    partition.note = (f"交接 #{handoff.handoff_id} 目标改派为 "
                                      f"{target or '（暂无）'}，实际投递边界 "
                                      f"{handoff.delivered_boundary} 保留")
                    self._event(partition, at, "handoff_retargeted",
                                handoff=handoff.handoff_id, to_member=target)
                    decisions.append({"partition": name, "decision": "handoff_retargeted",
                                      "handoff": handoff.handoff_id, "to_member": target})
                else:
                    decisions.append({"partition": name, "decision": "handoff_kept",
                                      "handoff": handoff.handoff_id})
                continue
            if partition.owner is None:
                partition.unassigned = reasons.get(name)
                if target is not None:
                    self._activate(partition, target, at, "新分配")
                    decisions.append({"partition": name, "decision": "activated",
                                      "owner": target, "assignment": partition.assignment})
                else:
                    detail = partition.unassigned["detail"] if partition.unassigned else ""
                    partition.note = f"留空：{detail}"
                    decisions.append({"partition": name, "decision": "unassigned",
                                      "reason": partition.unassigned})
                continue
            if target == partition.owner:
                partition.unassigned = None
                partition.note = (f"仍由 {partition.owner} 持有：资格与容量未变，"
                                  "无需移动，投递不中断")
                decisions.append({"partition": name, "decision": "kept",
                                  "owner": partition.owner})
                continue
            # 撤回当前所有者，建立移交承诺；只有被撤回的分区停止新批次。
            handoff = Handoff(
                handoff_id=partition.next_handoff_id,
                from_member=partition.owner,
                from_assignment=partition.assignment,
                to_member=target,
                to_assignment=partition.assignment + 1,
                delivered_boundary=partition.delivered,
                created_at=at,
                lease_deadline=at + self.drain_ttl,
                forced=bool(force_move and force_move.get(name) == partition.owner),
            )
            partition.next_handoff_id += 1
            partition.pending = handoff
            partition.unassigned = None
            partition.note = (f"已从 {handoff.from_member} 撤回 → "
                              f"{target or '（暂无合格目标）'}（交接 #{handoff.handoff_id}，"
                              f"实际投递边界 {handoff.delivered_boundary}）")
            self._event(partition, at, "revoked", handoff=handoff.handoff_id,
                        from_member=handoff.from_member, to_member=target,
                        delivered_boundary=handoff.delivered_boundary)
            decisions.append({"partition": name, "decision": "revoked",
                              "handoff": handoff.handoff_id,
                              "from_member": handoff.from_member, "to_member": target,
                              "delivered_boundary": handoff.delivered_boundary})
            self._check_trim_block(partition, at)
            if partition.pending is not None and partition.pending.state != "blocked" \
                    and partition.committed >= handoff.delivered_boundary:
                # 旧成员在撤回前已确认全部在途批次，交接当场完成。
                self._complete_handoff(partition, at, "drained")
        return decisions

    def _compute_desired(self, force_move=None):
        """目标归属：稳定优先，资格与容量是硬约束，重要性高的分区先落位。"""
        force_move = force_move or {}
        members = {m.id: m for m in self.members.values() if m.slots > 0}
        # 管理员强制撤回的分区，在交接完成前不得再派回原成员；
        # 本次强制挪动的分区同样不得派回。
        banned = {
            p.name: p.pending.from_member
            for p in self.partitions.values()
            if p.pending is not None and p.pending.forced
        }
        banned.update(force_move)
        keep = {}
        for partition in self.partitions.values():
            if partition.pending is not None or partition.owner is None:
                continue
            member = members.get(partition.owner)
            if member is None or partition.format not in member.formats:
                continue
            if force_move.get(partition.name) == partition.owner:
                continue
            keep[partition.name] = partition.owner
        loads = {mid: [] for mid in members}
        for pname, mid in keep.items():
            loads[mid].append(pname)
        # 超槽成员必须挪出：优先挪“能在别处安置”的，其次重要性低的。
        for mid in sorted(loads):
            member = members[mid]
            while len(loads[mid]) > member.slots:
                victim = self._pick_eviction(loads[mid], mid, members, loads)
                loads[mid].remove(victim)
                del keep[victim]
        # 安置待落位分区：重要性高的优先；容量不足时低优先级分区让位。
        queue = [(-p.priority, p.name) for p in self.partitions.values()
                 if p.name not in keep]
        queue.sort()
        index = 0
        while index < len(queue):
            _, pname = queue[index]
            index += 1
            if pname in keep:
                continue
            partition = self.partitions[pname]
            if self._place_one(partition, members, loads, keep, banned):
                continue
            victim = self._pick_preemption(partition, members, loads, banned)
            if victim is None:
                continue
            vname, vmid = victim
            loads[vmid].remove(vname)
            del keep[vname]
            keep[pname] = vmid
            loads[vmid].append(pname)
            bisect.insort(queue, (-self.partitions[vname].priority, vname))
        desired = dict(keep)
        reasons = {}
        for partition in self.partitions.values():
            if partition.name not in keep:
                desired[partition.name] = None
                reasons[partition.name] = self._unassigned_reason(partition, members)
        return desired, reasons

    def _pick_eviction(self, held, owner_id, members, loads):
        def relocatable(pname):
            partition = self.partitions[pname]
            return any(
                mid != owner_id and partition.format in m.formats
                and len(loads[mid]) < m.slots
                for mid, m in members.items()
            )
        pool = [p for p in held if relocatable(p)] or list(held)
        return min(pool, key=lambda p: (self.partitions[p].priority, p))

    def _place_one(self, partition, members, loads, keep, banned):
        excluded = banned.get(partition.name)
        candidates = [
            mid for mid, m in members.items()
            if mid != excluded
            and partition.format in m.formats
            and len(loads[mid]) < m.slots
        ]
        if not candidates:
            return False
        candidates.sort(key=lambda mid: (-(members[mid].slots - len(loads[mid])), mid))
        chosen = candidates[0]
        keep[partition.name] = chosen
        loads[chosen].append(partition.name)
        return True

    def _pick_preemption(self, partition, members, loads, banned):
        excluded = banned.get(partition.name)
        best = None
        for mid, member in members.items():
            if mid == excluded or partition.format not in member.formats:
                continue
            for qname in loads[mid]:
                other = self.partitions[qname]
                if other.priority < partition.priority:
                    key = (other.priority, qname, mid)
                    if best is None or key < best[0]:
                        best = (key, qname, mid)
        if best is None:
            return None
        return best[1], best[2]

    def _unassigned_reason(self, partition, members):
        eligible = [mid for mid, m in members.items() if partition.format in m.formats]
        if not eligible:
            return {"code": "no_eligible_member",
                    "detail": f"没有成员声明可消费格式 {partition.format}，分区留空"}
        return {"code": "capacity_exhausted",
                "detail": f"可消费 {partition.format} 的成员 {eligible} 槽位已满，分区留空"}

    def _activate(self, partition, member_id, at, why):
        partition.owner = member_id
        partition.assignment += 1
        partition.delivered = partition.committed
        partition.unassigned = None
        partition.note = (f"{why}：{member_id} 以 assignment {partition.assignment} 接管，"
                          f"从位置 {partition.committed + 1} 起投递")
        self._event(partition, at, "activated", member=member_id,
                    assignment=partition.assignment, next_position=partition.committed + 1)

    def _check_trim_block(self, partition, at):
        handoff = partition.pending
        if handoff is not None:
            if handoff.state == "draining" and partition.committed + 1 < partition.low:
                handoff.state = "blocked"
                handoff.lost_from = partition.committed + 1
                handoff.lost_to = partition.low - 1
                self._event(partition, at, "handoff_blocked",
                            handoff=handoff.handoff_id,
                            lost_from=handoff.lost_from, lost_to=handoff.lost_to)
        elif partition.owner is not None and partition.blocked is None \
                and partition.delivered + 1 < partition.low:
            partition.blocked = {
                "lost_from": partition.committed + 1,
                "lost_to": partition.low - 1,
                "token_assignment": partition.assignment,
                "detail": "保留点越过未确认位置，组不能自行跳过，等待管理员 reset",
            }
            self._event(partition, at, "delivery_blocked",
                        lost_from=partition.committed + 1,
                        lost_to=partition.low - 1)

    # ---- 内部：视图辅助 ---------------------------------------------------

    def _partition_state(self, partition):
        if partition.pending is not None:
            return "blocked_handoff" if partition.pending.state == "blocked" else "handoff"
        if partition.blocked is not None:
            return "blocked"
        if partition.owner is None:
            return "unassigned"
        return "active"

    def _waiting_on(self, partition):
        handoff = partition.pending
        if handoff is not None:
            if handoff.state == "blocked":
                return (f"保留点 {partition.low} 越过未确认位置 "
                        f"[{handoff.lost_from},{handoff.lost_to}]，组不能自行跳过；"
                        f"等待管理员按交接 #{handoff.handoff_id} 授权 reset")
            return (f"等待 {handoff.from_member} 确认 through>={handoff.delivered_boundary}"
                    f"（已确认至 {partition.committed}）或明确交还；"
                    f"租约至 {handoff.lease_deadline}")
        if partition.blocked is not None:
            return (f"保留点 {partition.low} 越过未确认位置 "
                    f"[{partition.blocked['lost_from']},{partition.blocked['lost_to']}]，"
                    f"等待管理员 reset（核对标识 assignment "
                    f"{partition.blocked['token_assignment']}）")
        if partition.owner is None:
            return partition.unassigned["detail"] if partition.unassigned else "等待 rebalance"
        return (f"正常投递中，下一位置 {partition.delivered + 1}，"
                f"在途未确认 {partition.delivered - partition.committed} 条")
