"""协调器命令行入口：建组、成员更新、领取、撤回、交还、重置与运营视图。

状态落在 --store 指定的 JSON 文件中，进程重启（每次调用都是新进程）后
未决交接与分区进度原样恢复。示例：

    python3 -m coordinator.cli --store /tmp/cg.json create-group
    python3 -m coordinator.cli --store /tmp/cg.json partition --name p \
        --format v1 --priority 2 --low 1 --high 8
    python3 -m coordinator.cli --store /tmp/cg.json member --id old \
        --boot 1 --formats v1,v2 --slots 2
    python3 -m coordinator.cli --store /tmp/cg.json rebalance
    python3 -m coordinator.cli --store /tmp/cg.json poll --member old \
        --partition p --assignment 1
    python3 -m coordinator.cli --store /tmp/cg.json ack --member old \
        --partition p --assignment 1 --through 1
    python3 -m coordinator.cli --store /tmp/cg.json giveback --member old \
        --partition p --assignment 1
    python3 -m coordinator.cli --store /tmp/cg.json reset --partition p \
        --handoff 1 --from 2 --to 3
    python3 -m coordinator.cli --store /tmp/cg.json inspect --partition p
"""

from __future__ import annotations

import argparse
import json

from .core import DEFAULT_DRAIN_TTL
from .service import Coordinator


def _print(result):
    print(json.dumps(result, ensure_ascii=False, indent=2))


def _formats(text):
    return [item for item in (part.strip() for part in text.split(",")) if item]


def build_parser():
    parser = argparse.ArgumentParser(description="本地消息消费组协调器")
    parser.add_argument("--store", default="coordinator-state.json",
                        help="状态存储路径（默认 ./coordinator-state.json）")
    parser.add_argument("--group", default="default", help="消费组名")
    parser.add_argument("--drain-ttl", type=int, default=DEFAULT_DRAIN_TTL,
                        help="旧租期长度（秒）")
    commands = parser.add_subparsers(dest="command", required=True)

    commands.add_parser("create-group", help="建组（幂等）")

    partition = commands.add_parser("partition", help="发布/更新分区（含提升优先级、推进保留点）")
    partition.add_argument("--name", required=True)
    partition.add_argument("--format", required=True)
    partition.add_argument("--priority", type=int, required=True)
    partition.add_argument("--low", type=int, required=True)
    partition.add_argument("--high", type=int, required=True)

    member = commands.add_parser("member", help="成员上线/更新（slots=0 退出）")
    member.add_argument("--id", required=True)
    member.add_argument("--boot", type=int, required=True)
    member.add_argument("--formats", required=True, help="逗号分隔，如 v1,v2")
    member.add_argument("--slots", type=int, required=True)

    commands.add_parser("rebalance", help="触发重新协调")

    poll = commands.add_parser("poll", help="领取（每次最多 --limit 条）")
    poll.add_argument("--member", required=True)
    poll.add_argument("--partition", required=True)
    poll.add_argument("--assignment", type=int, required=True)
    poll.add_argument("--limit", type=int, default=1)

    ack = commands.add_parser("ack", help="确认到指定位置（连续）")
    ack.add_argument("--member", required=True)
    ack.add_argument("--partition", required=True)
    ack.add_argument("--assignment", type=int, required=True)
    ack.add_argument("--through", type=int, required=True)

    giveback = commands.add_parser("giveback", help="明确交还未处理部分")
    giveback.add_argument("--member", required=True)
    giveback.add_argument("--partition", required=True)
    giveback.add_argument("--assignment", type=int, required=True)

    revoke = commands.add_parser("revoke", help="管理员强制撤回某分区归属")
    revoke.add_argument("--partition", required=True)

    reset = commands.add_parser("reset", help="管理员按明确损失范围授权重置（绑定移交身份）")
    reset.add_argument("--partition", required=True)
    reset.add_argument("--handoff", type=int, required=True)
    reset.add_argument("--from", dest="loss_from", type=int, required=True)
    reset.add_argument("--to", dest="loss_to", type=int, required=True)

    commands.add_parser("restart", help="从存储恢复（演练重启）")

    inspect = commands.add_parser("inspect", help="按分区查看实际拥有者与等待中的确认")
    inspect.add_argument("--partition", required=True)

    commands.add_parser("status", help="组总览（成员、分区、未决交接）")

    handoffs = commands.add_parser("handoffs", help="交接台账（可分别查过期与交还）")
    handoffs.add_argument("--completed-by",
                          choices=["drained", "giveback", "expired", "reset", "rescinded"],
                          default=None)

    replay_cmd = commands.add_parser("replay", help="按 JSONL 演练记录驱动协调器")
    replay_cmd.add_argument("scenario")

    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    if args.command == "replay":
        from .replay import run
        run(args.scenario, drain_ttl=args.drain_ttl)
        return
    coord = Coordinator(args.store, group=args.group, drain_ttl=args.drain_ttl)
    if args.command == "create-group":
        result = coord.create_group()
    elif args.command == "partition":
        result = coord.declare_partition(
            name=args.name, format=args.format, priority=args.priority,
            low=args.low, high=args.high)
    elif args.command == "member":
        result = coord.upsert_member(
            member_id=args.id, boot=args.boot,
            formats=_formats(args.formats), slots=args.slots)
    elif args.command == "rebalance":
        result = coord.rebalance()
    elif args.command == "poll":
        result = coord.poll(member_id=args.member, partition=args.partition,
                            assignment=args.assignment, limit=args.limit)
    elif args.command == "ack":
        result = coord.ack(member_id=args.member, partition=args.partition,
                           assignment=args.assignment, through=args.through)
    elif args.command == "giveback":
        result = coord.giveback(member_id=args.member, partition=args.partition,
                                assignment=args.assignment)
    elif args.command == "revoke":
        result = coord.force_revoke(args.partition)
    elif args.command == "reset":
        result = coord.reset(partition=args.partition, handoff_id=args.handoff,
                             loss_from=args.loss_from, loss_to=args.loss_to)
    elif args.command == "restart":
        result = coord.restart()
    elif args.command == "inspect":
        result = coord.inspect(args.partition)
    elif args.command == "status":
        result = coord.status()
    elif args.command == "handoffs":
        result = coord.handoffs(completed_by=args.completed_by)
    else:  # pragma: no cover - argparse 已拦截
        raise SystemExit(f"未知命令 {args.command}")
    _print(result)


if __name__ == "__main__":
    main()
