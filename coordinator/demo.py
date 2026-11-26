"""本地合成演练：观察领取、确认、撤回、交还、保留点阻塞、重置与重启恢复。

用法（在仓库根目录）：

    python3 -m coordinator.demo
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

from .replay import decision_text
from .service import Coordinator

DRAIN_TTL = 5  # 演示用短租约，便于观察过期路径


def brief(result):
    if not result.get("ok"):
        error = result["error"]
        return f"拒绝[{error['code']}] {error.get('detail', '')}"
    if "messages" in result:
        got = "、".join(f"{m['partition']}#{m['position']}" for m in result["messages"])
        return (f"领取 {got}（确认至 {result['committed']}）" if got
                else f"无新消息（已投至 {result['delivered']}）")
    if "decisions" in result:
        return "；".join(decision_text(d) for d in result["decisions"])
    if result.get("restarted"):
        return "恢复：" + "；".join(
            f"组 {g['group']} 未决交接 {len(g['pending_handoffs'])} 进度 {g['committed']}"
            for g in result["groups"])
    if result.get("handoff_completed"):
        return (f"确认至 {result['committed']}；交接完成（{result['handoff_completed']}），"
                f"{result['owner']} 以 a{result['assignment']} 接管")
    if result.get("completed") == "giveback":
        owner = result.get("owner")
        tail = f"{owner} 以 a{result['assignment']} 接管" if owner else "分区暂无人接管"
        return f"交还生效，交接完成（giveback），{tail}"
    if "authorized_loss" in result:
        loss = result["authorized_loss"]
        return (f"授权损失 [{loss['from']},{loss['to']}]，确认推进至 "
                f"{result['committed']}，{result.get('owner')} 以 "
                f"a{result.get('assignment')} 接管")
    if "committed" in result:
        extra = f"，{result['note']}" if result.get("note") else ""
        return f"确认至 {result['committed']}{extra}"
    return result.get("note") or json.dumps(result, ensure_ascii=False)


def show(at, action, result):
    print(f"  at={at:<3} {action:<46} → {brief(result)}")


def phase(title):
    print(f"\n=== {title} ===")


def main():
    scratch = tempfile.TemporaryDirectory(prefix="coordinator-demo-")
    coord = Coordinator(Path(scratch.name) / "state.json", drain_ttl=DRAIN_TTL)
    coord.create_group()
    print("本地合成演练开始（状态目录为临时目录，仅供观察）")

    phase("A. 建组：发布分区、成员声明能力（格式 + 槽位）")
    show(0, "发布 p(v1, prio=2, [1,6])",
         coord.declare_partition("p", "v1", 2, 1, 6, at=0))
    show(0, "发布 q(v2, prio=1, [1,6])",
         coord.declare_partition("q", "v2", 1, 1, 6, at=0))
    show(0, "发布 r(v3, prio=1, [1,6])",
         coord.declare_partition("r", "v3", 1, 1, 6, at=0))
    show(1, "成员 m1 上线 formats=[v1,v2] slots=2",
         coord.upsert_member("m1", 1, ["v1", "v2"], 2, at=1))
    show(1, "成员 m2 上线 formats=[v1] slots=1",
         coord.upsert_member("m2", 1, ["v1"], 1, at=1))
    show(2, "rebalance", coord.rebalance(at=2))
    view = coord.inspect("r")
    print(f"  运营查看 r：{view['state']}｜{view['waiting_on']}（无资格留空，不硬塞给看不懂的成员）")

    phase("B. 领取与确认（已领取 ≠ 已处理）")
    show(3, "m1 领取 p", coord.poll("m1", "p", 1, at=3))
    show(3, "m1 再领 p", coord.poll("m1", "p", 1, at=3))
    show(3, "m1 确认 p through=1", coord.ack("m1", "p", 1, 1, at=3))
    show(3, "m1 领取 q", coord.poll("m1", "q", 1, at=3))
    show(3, "m1 确认 q through=1", coord.ack("m1", "q", 1, 1, at=3))

    phase("C. 成员更新（m1 槽位 2→1）：只挪动必要的分区")
    show(4, "m1 更新 slots=1", coord.upsert_member("m1", 1, ["v1", "v2"], 1, at=4))
    show(5, "rebalance", coord.rebalance(at=5))
    show(6, "m1 继续领取 q（未中断）", coord.poll("m1", "q", 1, at=6))
    show(6, "m1 再领 p（已撤回）", coord.poll("m1", "p", 1, at=6))

    phase("D. 旧成员完成已取批次，新成员从已确认下一项开始")
    show(7, "m1 确认 p through=2（在租约内）",
         coord.ack("m1", "p", 1, 2, at=7))
    show(8, "m2 领取 p（assignment 2）", coord.poll("m2", "p", 2, at=8))

    phase("E. 明确交还：未处理部分交回并重投")
    show(9, "m2 领取 p（不确认）", coord.poll("m2", "p", 2, at=9))
    show(10, "m2 退出（slots=0）", coord.upsert_member("m2", 1, ["v1"], 0, at=10))
    show(11, "m1 恢复 slots=2", coord.upsert_member("m1", 1, ["v1", "v2"], 2, at=11))
    show(12, "rebalance", coord.rebalance(at=12))
    show(13, "m2 交还 p（giveback）", coord.giveback("m2", "p", 2, at=13))
    show(14, "m1 领取 p（未确认部分重投）", coord.poll("m1", "p", 3, at=14))
    show(14, "m1 再领 p", coord.poll("m1", "p", 3, at=14))
    show(14, "m1 确认 p through=4", coord.ack("m1", "p", 3, 4, at=14))

    phase("F. 待交接时保留点越过未确认位置：组不自行跳过，管理员按范围重置")
    show(15, "m1 领取 p（不确认）", coord.poll("m1", "p", 3, at=15))
    show(16, "m2 重新上线", coord.upsert_member("m2", 1, ["v1"], 1, at=16))
    show(17, "m1 更新 slots=1", coord.upsert_member("m1", 1, ["v1", "v2"], 1, at=17))
    show(17, "rebalance", coord.rebalance(at=17))
    show(18, "p 保留点推进到 6",
         coord.declare_partition("p", "v1", 2, 6, 6, at=18))
    view = coord.inspect("p")
    print(f"  运营查看 p：{view['state']}｜{view['waiting_on']}")
    show(20, "m1 交还 p（已登记，仍待重置）", coord.giveback("m1", "p", 3, at=20))
    show(21, "reset 绑错移交身份 handoff=2",
         coord.reset("p", 2, 5, 5, at=21))
    show(21, "reset 损失范围不精确 [5,4]",
         coord.reset("p", 3, 5, 4, at=21))
    show(22, "reset handoff=3 损失[5,5]",
         coord.reset("p", 3, 5, 5, at=22))
    show(23, "m2 领取 p（assignment 4）", coord.poll("m2", "p", 4, at=23))

    phase("G. 重启恢复：未决交接与分区进度原样保留；旧租期过期后迟到确认被拒")
    show(24, "成员 m3 上线 formats=[v2] slots=1",
         coord.upsert_member("m3", 1, ["v2"], 1, at=24))
    show(24, "m1 退出（slots=0）", coord.upsert_member("m1", 1, ["v1", "v2"], 0, at=24))
    show(24, "rebalance", coord.rebalance(at=24))
    show(25, "协调器重启", coord.restart())
    show(26, "m1 再领 q（已撤回）", coord.poll("m1", "q", 1, at=26))
    show(30, "m3 领取 q（租约已过期，交接按过期完成）",
         coord.poll("m3", "q", 2, at=30))
    show(31, "m1 迟到确认 q through=2", coord.ack("m1", "q", 1, 2, at=31))

    phase("H. 管理员提升优先级：容量不足时重要分区先获得安排")
    show(32, "发布 t(v1, prio=9, [1,2])",
         coord.declare_partition("t", "v1", 9, 1, 2, at=32))
    show(33, "rebalance", coord.rebalance(at=33))
    show(34, "m2 交还 p（让位后无合格容量）", coord.giveback("m2", "p", 4, at=34))
    view = coord.inspect("p")
    print(f"  运营查看 p：{view['state']}｜{view['waiting_on']}")

    phase("运营总览与交接台账（过期与交还可分别查）")
    status = coord.status()
    for item in status["partitions"]:
        print(f"  {item['partition']:<2} {item['state']:<15} 拥有者={item['owner']}"
              f"（a{item['assignment']}）确认至 {item['committed']}｜{item['note']}")
    for how in ("giveback", "expired", "reset"):
        done = coord.handoffs(completed_by=how)["completed"]
        print(f"  台账[{how}]："
              + ("；".join(f"{r['partition']}#{r['handoff']} {r['from_member']}→{r['to_member']}"
                           for r in done) or "（无）"))
    print("\n演练结束。")


if __name__ == "__main__":
    main()
