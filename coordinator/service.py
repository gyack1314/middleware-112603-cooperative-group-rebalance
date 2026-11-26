"""协调器服务层：多组管理、状态持久化与重启恢复。

状态在每次变更后原子写入 store_path（临时文件 + os.replace）。
进程重启后新建 Coordinator 实例即可从未决交接与分区进度的原样状态继续。
"""

from __future__ import annotations

import json
import os
import time

from .core import DEFAULT_DRAIN_TTL, Group


class Coordinator:
    """面向本地持续运行的消费组协调器。"""

    def __init__(self, store_path=None, group="default", drain_ttl=DEFAULT_DRAIN_TTL):
        self.store_path = None if store_path in (None, ":memory:") else str(store_path)
        self.default_group = group
        self.drain_ttl = drain_ttl
        self.groups = {}
        if self.store_path and os.path.exists(self.store_path):
            self._load()

    # ---- 持久化 ---------------------------------------------------------

    def _load(self):
        with open(self.store_path, encoding="utf-8") as handle:
            raw = json.load(handle)
        self.groups = {
            name: Group.from_dict(data) for name, data in raw.get("groups", {}).items()
        }

    def _save(self):
        if not self.store_path:
            return
        payload = {
            "version": 1,
            "groups": {name: group.to_dict() for name, group in self.groups.items()},
        }
        tmp_path = self.store_path + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
        os.replace(tmp_path, self.store_path)

    def restart(self):
        """重启：丢弃内存状态，从存储恢复。返回恢复摘要供运营核对。"""
        if not self.store_path:
            return {"ok": False,
                    "error": {"code": "no_store", "detail": "内存模式无存储可恢复"}}
        if os.path.exists(self.store_path):
            self._load()
        summary = []
        for name, group in sorted(self.groups.items()):
            summary.append({
                "group": name,
                "members": len(group.members),
                "partitions": len(group.partitions),
                "pending_handoffs": [
                    {"partition": p.name, "handoff": p.pending.handoff_id,
                     "from_member": p.pending.from_member,
                     "to_member": p.pending.to_member,
                     "state": p.pending.state}
                    for p in sorted(group.partitions.values(), key=lambda x: x.name)
                    if p.pending is not None
                ],
                "committed": {
                    p.name: p.committed
                    for p in sorted(group.partitions.values(), key=lambda x: x.name)
                },
            })
        return {"ok": True, "restarted": True, "groups": summary}

    # ---- 组管理 ---------------------------------------------------------

    @staticmethod
    def _now(at):
        return int(time.time()) if at is None else at

    def create_group(self, name=None):
        name = name or self.default_group
        if name not in self.groups:
            self.groups[name] = Group(name, self.drain_ttl)
            self._save()
        return {"ok": True, "group": name}

    def _group(self, name):
        name = name or self.default_group
        if name not in self.groups:
            self.groups[name] = Group(name, self.drain_ttl)
        return self.groups[name]

    # ---- 操作入口（每次变更即持久化） ------------------------------------

    def declare_partition(self, name, format, priority, low, high, at=None, group=None):
        result = self._group(group).declare_partition(
            self._now(at), name, format, priority, low, high)
        self._save()
        return result

    def upsert_member(self, member_id, boot, formats, slots, at=None, group=None):
        result = self._group(group).upsert_member(
            self._now(at), member_id, boot, formats, slots)
        self._save()
        return result

    def rebalance(self, at=None, group=None):
        result = self._group(group).rebalance(self._now(at))
        self._save()
        return result

    def force_revoke(self, partition, at=None, group=None):
        result = self._group(group).force_revoke(self._now(at), partition)
        self._save()
        return result

    def poll(self, member_id, partition, assignment, limit=1, at=None, group=None):
        result = self._group(group).poll(
            self._now(at), member_id, partition, assignment, limit=limit)
        self._save()
        return result

    def ack(self, member_id, partition, assignment, through, at=None, group=None):
        result = self._group(group).ack(
            self._now(at), member_id, partition, assignment, through)
        self._save()
        return result

    def giveback(self, member_id, partition, assignment, at=None, group=None):
        result = self._group(group).giveback(
            self._now(at), member_id, partition, assignment)
        self._save()
        return result

    def reset(self, partition, handoff_id, loss_from, loss_to, at=None, group=None):
        result = self._group(group).reset(
            self._now(at), partition, handoff_id, loss_from, loss_to)
        self._save()
        return result

    # ---- 运营视图（只读，不落盘） -----------------------------------------

    def inspect(self, partition, group=None):
        return self._group(group).inspect(partition)

    def status(self, group=None):
        return self._group(group).status()

    def handoffs(self, completed_by=None, group=None):
        return self._group(group).handoffs(completed_by=completed_by)
