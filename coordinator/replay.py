"""按演练记录驱动协调器，逐条打印可观察结果（领取、确认、交接、重置等）。

用法（在仓库根目录）：

    python3 -m coordinator.replay examples/changing-one-partition.jsonl
    python3 -m coordinator.replay examples/trimmed-before-handoff.jsonl
"""

from __future__ import annotations

import argparse
import json
import tempfile
from pathlib import Path

from scenario_reader import iter_messages

from .core import DEFAULT_DRAIN_TTL
from .service import Coordinator

DEFAULT_KINDS = [
    "ack", "giveback", "inspect", "member", "partition",
    "poll", "rebalance", "reset", "restart",
]


def apply_record(coord, at, kind, record):
    """把一条参与方消息应用到协调器，返回结果字典。"""
    if kind == "partition":
        return coord.declare_partition(
            name=record["partition"], format=record["format"],
            priority=record["priority"], low=record["low"],
            high=record["high"], at=at)
    if kind == "member":
        return coord.upsert_member(
            member_id=record["id"], boot=record["boot"],
            formats=record["formats"], slots=record["slots"], at=at)
    if kind == "rebalance":
        return coord.rebalance(at=at)
    if kind == "poll":
        return coord.poll(
            member_id=record["member"], partition=record["partition"],
            assignment=record["assignment"], at=at)
    if kind == "ack":
        return coord.ack(
            member_id=record["member"], partition=record["partition"],
            assignment=record["assignment"], through=record["through"], at=at)
    if kind == "giveback":
        return coord.giveback(
            member_id=record["member"], partition=record["partition"],
            assignment=record["assignment"], at=at)
    if kind == "reset":
        return coord.reset(
            partition=record["partition"], handoff_id=record["handoff"],
            loss_from=record["from"], loss_to=record["to"], at=at)
    if kind == "restart":
        return coord.restart()
    if kind == "inspect":
        return coord.inspect(record["partition"])
    return {"ok": False, "error": {"code": "unknown_kind", "detail": kind}}


def describe_target(kind, record):
    if kind == "partition":
        return f"{record['partition']} fmt={record['format']} prio={record['priority']} [{record['low']},{record['high']}]"
    if kind == "member":
        return f"{record['id']} boot={record['boot']} slots={record['slots']} {record['formats']}"
    if kind in ("poll", "giveback"):
        return f"{record['member']} {record['partition']}@a{record['assignment']}"
    if kind == "ack":
        return f"{record['member']} {record['partition']}@a{record['assignment']} through={record['through']}"
    if kind == "reset":
        return f"{record['partition']} handoff={record['handoff']} 损失[{record['from']},{record['to']}]"
    if kind == "inspect":
        return record["partition"]
    return ""


def decision_text(decision):
    kind = decision["decision"]
    name = decision["partition"]
    if kind == "activated":
        return f"{name} → {decision['owner']}（a{decision['assignment']}）"
    if kind == "kept":
        return f"{name} 留 {decision['owner']}（未中断）"
    if kind == "revoked":
        target = decision["to_member"] or "（暂无目标）"
        return (f"{name} 撤回 {decision['from_member']}→{target}"
                f"（交接 #{decision['handoff']}，边界 {decision['delivered_boundary']}）")
    if kind == "unassigned":
        reason = decision.get("reason") or {}
        return f"{name} 留空（{reason.get('code', '?')}）"
    if kind == "handoff_rescinded":
        return f"{name} 撤回撤销（交接 #{decision['handoff']}）"
    if kind == "handoff_retargeted":
        return f"{name} 交接 #{decision['handoff']} 目标改派 {decision['to_member']}"
    if kind == "handoff_kept":
        return f"{name} 交接 #{decision['handoff']} 进行中"
    return f"{name} {kind}"


def describe_result(kind, record, result):
    if not result.get("ok"):
        error = result["error"]
        return f"拒绝[{error['code']}] {error.get('detail', '')}"
    if kind == "partition":
        return result.get("note", "已登记")
    if kind == "member":
        return result.get("note", "已登记")
    if kind == "rebalance":
        return "；".join(decision_text(d) for d in result["decisions"]) or "无分区"
    if kind == "poll":
        if not result["messages"]:
            return (f"无新消息（已投至 {result['delivered']}，"
                    f"high={result['high']}，确认至 {result['committed']}）")
        got = "、".join(
            f"{m['partition']}#{m['position']}" for m in result["messages"])
        return f"领取 {got}（确认至 {result['committed']}）"
    if kind == "ack":
        text = f"确认至 {result['committed']}"
        if result.get("handoff_completed"):
            text += (f"；交接完成（{result['handoff_completed']}），"
                     f"{result['owner']} 以 a{result['assignment']} 接管")
        elif result.get("note"):
            text += f"，{result['note']}"
        return text
    if kind == "giveback":
        if result.get("completed"):
            return (f"交还生效，交接完成（giveback），"
                    f"{result['owner']} 以 a{result['assignment']} 接管")
        return result.get("note", "已登记")
    if kind == "reset":
        loss = result["authorized_loss"]
        return (f"授权损失 [{loss['from']},{loss['to']}]，确认推进至 "
                f"{result['committed']}，{result.get('owner')} 以 "
                f"a{result.get('assignment')} 接管")
    if kind == "restart":
        parts = []
        for group in result["groups"]:
            pending = group["pending_handoffs"]
            parts.append(
                f"组 {group['group']}：分区 {group['partitions']}，"
                f"未决交接 {len(pending)}，进度 {group['committed']}")
        return "恢复：" + "；".join(parts)
    if kind == "inspect":
        return (f"{result['partition']} 状态={result['state']} 拥有者={result['owner']}"
                f"（a{result['assignment']}）确认至 {result['committed']}"
                f"｜{result['waiting_on']}")
    return json.dumps(result, ensure_ascii=False)


def run(path, store_path=None, drain_ttl=DEFAULT_DRAIN_TTL, emit=print):
    """逐条应用演练记录，返回 (协调器, 每行结果) 便于测试核对。"""
    path = Path(path)
    scratch = None
    if store_path is None:
        scratch = tempfile.TemporaryDirectory(prefix="coordinator-replay-")
        store_path = Path(scratch.name) / "state.json"
    coord = Coordinator(store_path, drain_ttl=drain_ttl)
    coord.create_group()
    registry = path.parent / "message_types.json"
    supported = (set(json.loads(registry.read_text(encoding="utf-8")))
                 if registry.exists() else set(DEFAULT_KINDS))
    lines = []
    for row in iter_messages(path, supported):
        at, kind, record = row["at"], row["kind"], row["record"]
        result = apply_record(coord, at, kind, record)
        line = (f"at={at:<4} {kind:<9} {describe_target(kind, record):<30} → "
                f"{describe_result(kind, record, result)}")
        lines.append((at, kind, record, result))
        emit(line)
        if kind == "inspect" and result.get("ok"):
            view = {key: value for key, value in result.items() if key != "ok"}
            for view_line in json.dumps(view, ensure_ascii=False, indent=2).splitlines():
                emit("        " + view_line)
    return coord, lines


def main(argv=None):
    parser = argparse.ArgumentParser(description="按演练记录驱动消费组协调器")
    parser.add_argument("scenario", type=Path, help="JSONL 演练记录")
    parser.add_argument("--store", type=Path, default=None,
                        help="状态存储路径（默认临时目录，restart 也在其中恢复）")
    parser.add_argument("--drain-ttl", type=int, default=DEFAULT_DRAIN_TTL,
                        help="旧租期长度（秒点）")
    args = parser.parse_args(argv)
    run(args.scenario, store_path=args.store, drain_ttl=args.drain_ttl)


if __name__ == "__main__":
    main()
