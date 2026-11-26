"""协调器关键语义的单元测试（标准库 unittest）。

运行：python3 -m unittest discover -v
"""

import json
import tempfile
import unittest
from pathlib import Path

from coordinator import Coordinator
from coordinator.replay import apply_record
from scenario_reader import iter_messages

EXAMPLES = Path(__file__).parent / "examples"


def apply_file(path, coord, probes=None):
    """逐条应用演练记录；probes 可按 at 注入额外断言动作。"""
    registry = json.loads((path.parent / "message_types.json").read_text(encoding="utf-8"))
    results = []
    for row in iter_messages(path, set(registry)):
        if probes and row["at"] in probes:
            probes[row["at"]]()
        results.append(apply_record(coord, row["at"], row["kind"], row["record"]))
    return results


class ContractReplayTests(unittest.TestCase):
    """直接回放仓库自带的演练记录，核对每一步可观察结果。"""

    def setUp(self):
        self.scratch = tempfile.TemporaryDirectory()
        self.addCleanup(self.scratch.cleanup)
        self.store = str(Path(self.scratch.name) / "state.json")

    def test_changing_one_partition(self):
        coord = Coordinator(self.store)
        results = apply_file(EXAMPLES / "changing-one-partition.jsonl", coord)

        # at=2 rebalance：p、q 都归 old（assignment 1）
        decisions = {d["partition"]: d for d in results[3]["decisions"]}
        self.assertEqual(decisions["p"]["decision"], "activated")
        self.assertEqual(decisions["p"]["owner"], "old")
        self.assertEqual(decisions["q"]["owner"], "old")

        # at=3 old 领取 p#1
        self.assertTrue(results[4]["ok"])
        self.assertEqual([m["position"] for m in results[4]["messages"]], [1])

        # at=6 rebalance：q 留 old 不中断；p 撤回 old→new（交接 #1，边界 1）
        decisions = {d["partition"]: d for d in results[7]["decisions"]}
        self.assertEqual(decisions["q"]["decision"], "kept")
        self.assertEqual(decisions["p"]["decision"], "revoked")
        self.assertEqual(decisions["p"]["handoff"], 1)
        self.assertEqual(decisions["p"]["from_member"], "old")
        self.assertEqual(decisions["p"]["to_member"], "new")
        self.assertEqual(decisions["p"]["delivered_boundary"], 1)

        # at=7 old 继续领取 q#1（未受 p 撤回影响）
        self.assertTrue(results[8]["ok"])
        self.assertEqual([m["position"] for m in results[8]["messages"]], [1])

        # at=8 old 确认 p through 1：交接因确认完毕而完成，new 以 assignment 2 接管
        self.assertTrue(results[9]["ok"])
        self.assertEqual(results[9]["committed"], 1)
        self.assertEqual(results[9]["handoff_completed"], "drained")
        self.assertEqual(results[9]["owner"], "new")
        self.assertEqual(results[9]["assignment"], 2)

        # at=9 giveback：交接已完成，幂等空操作
        self.assertTrue(results[10]["ok"])
        self.assertIn("幂等", results[10]["note"])

        # at=10 restart：进度与归属从存储恢复
        self.assertTrue(results[11]["restarted"])

        # at=11 new 以 assignment 2 领取 p#2（从已确认下一项开始）
        self.assertTrue(results[12]["ok"])
        self.assertEqual([m["position"] for m in results[12]["messages"]], [2])

        view = coord.inspect("p")
        self.assertEqual((view["owner"], view["assignment"], view["committed"]), ("new", 2, 1))
        view = coord.inspect("q")
        self.assertEqual((view["owner"], view["assignment"]), ("old", 1))

    def test_trimmed_before_handoff(self):
        coord = Coordinator(self.store)
        probes = {
            # reset 之前先验证：绑错移交身份、损失范围不精确都被拒绝
            10: lambda: self._reset_probes(coord),
        }
        results = apply_file(EXAMPLES / "trimmed-before-handoff.jsonl", coord, probes)

        # at=3 a 领取 x#1
        self.assertEqual([m["position"] for m in results[3]["messages"]], [1])

        # at=6 rebalance：x 撤回 a→b（交接 #1，边界 1）
        decisions = {d["partition"]: d for d in results[6]["decisions"]}
        self.assertEqual(decisions["x"]["decision"], "revoked")
        self.assertEqual(decisions["x"]["handoff"], 1)
        self.assertEqual(decisions["x"]["delivered_boundary"], 1)

        # at=7 保留点推进到 4：越过未确认位置，交接被阻塞（declare 结果自带状态）
        self.assertEqual(results[7]["state"], "blocked_handoff")

        # at=8 giveback：登记交还，但仍等待管理员 reset
        self.assertTrue(results[8]["ok"])
        self.assertIn("reset", results[8]["note"])

        # at=9 inspect：运营可见等待哪次确认 / 哪个 reset
        self.assertEqual(results[9]["state"], "blocked_handoff")
        pending = results[9]["pending_handoff"]
        self.assertEqual((pending["lost_from"], pending["lost_to"]), (1, 3))
        self.assertIn("交接 #1", results[9]["waiting_on"])

        # at=10 reset handoff=1 损失[1,3]：确认推进至 3，b 以 assignment 2 接管
        self.assertTrue(results[10]["ok"])
        self.assertEqual(results[10]["committed"], 3)
        self.assertEqual((results[10]["owner"], results[10]["assignment"]), ("b", 2))

        # at=11 b 领取 x#4（从保留点之后开始）
        self.assertEqual([m["position"] for m in results[11]["messages"]], [4])

        # 交接台账：完成方式为 reset，且登记过 giveback
        done = coord.handoffs(completed_by="reset")["completed"]
        self.assertEqual(len(done), 1)
        self.assertTrue(done[0]["giveback_received"])
        self.assertEqual(done[0]["authorized_loss"], {"from": 1, "to": 3})

    def _reset_probes(self, coord):
        wrong_id = coord.reset("x", 2, 1, 3, at=10)
        self.assertEqual(wrong_id["error"]["code"], "handoff_mismatch")
        wrong_range = coord.reset("x", 1, 1, 2, at=10)
        self.assertEqual(wrong_range["error"]["code"], "loss_range_mismatch")
        self.assertEqual(wrong_range["error"]["expected"], {"from": 1, "to": 3})


class AssignmentTests(unittest.TestCase):
    def setUp(self):
        self.coord = Coordinator(":memory:")

    def test_ineligible_partition_left_empty_with_reason(self):
        self.coord.declare_partition("p1", "v9", 1, 1, 5, at=0)
        self.coord.upsert_member("m1", 1, ["v1"], 2, at=0)
        self.coord.rebalance(at=1)
        view = self.coord.inspect("p1")
        self.assertEqual(view["state"], "unassigned")
        self.assertEqual(view["unassigned"]["code"], "no_eligible_member")

    def test_no_balance_onto_format_blind_member(self):
        # m2 槽位充足但看不懂 v1：不能因偏好均衡把 q 交给它
        self.coord.declare_partition("p", "v1", 1, 1, 5, at=0)
        self.coord.declare_partition("q", "v1", 1, 1, 5, at=0)
        self.coord.upsert_member("m1", 1, ["v1"], 1, at=0)
        self.coord.upsert_member("m2", 1, ["v2"], 10, at=0)
        self.coord.rebalance(at=1)
        self.assertEqual(self.coord.inspect("p")["owner"], "m1")
        view = self.coord.inspect("q")
        self.assertEqual(view["state"], "unassigned")
        self.assertEqual(view["unassigned"]["code"], "capacity_exhausted")

    def test_priority_preemption_when_capacity_scarce(self):
        self.coord.declare_partition("low", "v1", 1, 1, 5, at=0)
        self.coord.upsert_member("m1", 1, ["v1"], 1, at=0)
        self.coord.rebalance(at=1)
        self.assertEqual(self.coord.inspect("low")["owner"], "m1")
        self.coord.poll("m1", "low", 1, at=1)  # 在途未确认，交接不会当场完成
        # 更重要的分区出现：容量不足时优先获得安排
        self.coord.declare_partition("high", "v1", 9, 1, 5, at=2)
        self.coord.rebalance(at=3)
        self.assertEqual(self.coord.inspect("high")["owner"], "m1")
        pending = self.coord.inspect("low")["pending_handoff"]
        self.assertEqual(pending["from_member"], "m1")
        self.assertIsNone(pending["to_member"])
        # 旧成员交还后，低优先级分区留空并给出容量原因
        self.coord.giveback("m1", "low", 1, at=4)
        view = self.coord.inspect("low")
        self.assertEqual(view["state"], "unassigned")
        self.assertEqual(view["unassigned"]["code"], "capacity_exhausted")

    def test_rebalance_is_stable_and_revocation_stops_only_that_partition(self):
        self.coord.declare_partition("p", "v1", 2, 1, 5, at=0)
        self.coord.declare_partition("q", "v2", 1, 1, 5, at=0)
        self.coord.upsert_member("m1", 1, ["v1", "v2"], 2, at=0)
        self.coord.upsert_member("m2", 1, ["v1"], 1, at=0)
        self.coord.rebalance(at=1)
        self.coord.poll("m1", "p", 1, at=2)  # p#1 在途未确认
        # m1 收缩到 1 槽：只有 p 需要挪动
        self.coord.upsert_member("m1", 1, ["v1", "v2"], 1, at=2)
        self.coord.rebalance(at=3)
        # q 未中断，继续投递
        fetched = self.coord.poll("m1", "q", 1, at=4)
        self.assertTrue(fetched["ok"])
        self.assertEqual([m["position"] for m in fetched["messages"]], [1])
        # p 已撤回：旧成员停止新批次，新成员尚未接管
        self.assertEqual(self.coord.poll("m1", "p", 1, at=4)["error"]["code"], "revoked")
        self.assertEqual(self.coord.poll("m2", "p", 2, at=4)["error"]["code"],
                         "handoff_pending")

    def test_admin_priority_raise_reschedules(self):
        self.coord.declare_partition("p", "v1", 1, 1, 5, at=0)
        self.coord.declare_partition("q", "v1", 1, 1, 5, at=0)
        self.coord.upsert_member("m1", 1, ["v1"], 1, at=0)
        self.coord.rebalance(at=1)
        self.assertEqual(self.coord.inspect("p")["owner"], "m1")
        self.assertEqual(self.coord.inspect("q")["state"], "unassigned")
        self.coord.poll("m1", "p", 1, at=1)  # 在途未确认
        # 管理员提升 q 的优先级 → 重新协调后 q 优先获得安排
        self.coord.declare_partition("q", "v1", 5, 1, 5, at=2)
        self.coord.rebalance(at=3)
        self.assertEqual(self.coord.inspect("q")["owner"], "m1")
        self.assertEqual(self.coord.inspect("p")["pending_handoff"]["from_member"], "m1")


class HandoffTests(unittest.TestCase):
    def setUp(self):
        self.coord = Coordinator(":memory:", drain_ttl=5)

    def _owned_partition(self, name="p", high=5):
        self.coord.declare_partition(name, "v1", 1, 1, high, at=0)
        self.coord.upsert_member("m1", 1, ["v1"], 1, at=0)
        self.coord.rebalance(at=1)

    def test_drained_handoff_completes_and_new_owner_starts_after_committed(self):
        self._owned_partition()
        self.coord.poll("m1", "p", 1, at=2)
        self.coord.poll("m1", "p", 1, at=2)
        self.coord.ack("m1", "p", 1, 1, at=2)
        self.coord.upsert_member("m2", 1, ["v1"], 1, at=3)
        self.coord.upsert_member("m1", 1, ["v1"], 0, at=3)
        self.coord.rebalance(at=4)
        handoff = self.coord.inspect("p")["pending_handoff"]
        self.assertEqual(handoff["delivered_boundary"], 2)
        # 旧成员在租约内完成已取批次
        result = self.coord.ack("m1", "p", 1, 2, at=5)
        self.assertEqual(result["handoff_completed"], "drained")
        # 新成员只能从已确认的下一项开始
        fetched = self.coord.poll("m2", "p", 2, at=6)
        self.assertEqual([m["position"] for m in fetched["messages"]], [3])

    def test_giveback_redelivers_unprocessed(self):
        self._owned_partition()
        self.coord.poll("m1", "p", 1, at=2)
        self.coord.poll("m1", "p", 1, at=2)
        self.coord.ack("m1", "p", 1, 1, at=2)
        self.coord.upsert_member("m2", 1, ["v1"], 1, at=3)
        self.coord.upsert_member("m1", 1, ["v1"], 0, at=3)
        self.coord.rebalance(at=4)
        result = self.coord.giveback("m1", "p", 1, at=5)
        self.assertEqual(result["completed"], "giveback")
        self.assertEqual(self.coord.inspect("p")["committed"], 1)
        # 未处理部分 [2,2] 重新投递给新成员
        first = self.coord.poll("m2", "p", 2, at=6)
        second = self.coord.poll("m2", "p", 2, at=6)
        self.assertEqual([m["position"] for m in first["messages"]], [2])
        self.assertEqual([m["position"] for m in second["messages"]], [3])

    def test_expired_lease_rejects_late_ack_and_protects_new_owner_batches(self):
        self._owned_partition()
        self.coord.poll("m1", "p", 1, at=2)
        self.coord.poll("m1", "p", 1, at=2)
        self.coord.upsert_member("m2", 1, ["v1"], 1, at=3)
        self.coord.upsert_member("m1", 1, ["v1"], 0, at=3)
        self.coord.rebalance(at=10)  # 租约至 15
        # 旧成员既不确认也不交还；租约过期后新成员接管并重投
        fetched = self.coord.poll("m2", "p", 2, at=16)
        self.assertTrue(fetched["ok"])
        self.assertEqual([m["position"] for m in fetched["messages"]], [1])
        self.coord.poll("m2", "p", 2, at=16)
        self.coord.ack("m2", "p", 2, 2, at=16)
        # 旧租期的迟到确认不得越过新成员已取得的批次
        late = self.coord.ack("m1", "p", 1, 2, at=17)
        self.assertEqual(late["error"]["code"], "stale_assignment")
        self.assertEqual(late["error"]["handoff_completed_by"], "expired")
        self.assertEqual(self.coord.inspect("p")["committed"], 2)
        events = self.coord.inspect("p")["events"]
        self.assertTrue(any(e["kind"] == "late_ack_rejected" for e in events))

    def test_expired_and_giveback_separately_queryable(self):
        self.coord.declare_partition("x", "v1", 1, 1, 5, at=0)
        self.coord.declare_partition("y", "v1", 1, 1, 5, at=0)
        self.coord.upsert_member("m1", 1, ["v1"], 2, at=0)
        self.coord.rebalance(at=1)
        self.coord.poll("m1", "x", 1, at=2)
        self.coord.poll("m1", "y", 1, at=2)
        self.coord.upsert_member("m2", 1, ["v1"], 2, at=3)
        self.coord.upsert_member("m1", 1, ["v1"], 0, at=3)
        self.coord.rebalance(at=10)
        self.coord.giveback("m1", "x", 1, at=11)          # x 明确交还
        self.coord.poll("m2", "y", 2, at=20)              # y 租约过期
        given_back = self.coord.handoffs(completed_by="giveback")["completed"]
        expired = self.coord.handoffs(completed_by="expired")["completed"]
        self.assertEqual([r["partition"] for r in given_back], ["x"])
        self.assertEqual([r["partition"] for r in expired], ["y"])

    def test_ack_cannot_exceed_delivered(self):
        self._owned_partition()
        self.coord.poll("m1", "p", 1, at=2)
        result = self.coord.ack("m1", "p", 1, 2, at=2)
        self.assertEqual(result["error"]["code"], "beyond_delivered")
        self.assertEqual(self.coord.inspect("p")["committed"], 0)

    def test_stale_assignment_fencing(self):
        self._owned_partition()
        self.coord.poll("m1", "p", 1, at=2)
        self.coord.upsert_member("m2", 1, ["v1"], 1, at=3)
        self.coord.upsert_member("m1", 1, ["v1"], 0, at=3)
        self.coord.rebalance(at=4)
        self.coord.giveback("m1", "p", 1, at=5)
        # 旧代次的领取与确认都被栅栏
        self.assertEqual(self.coord.poll("m1", "p", 1, at=6)["error"]["code"],
                         "not_owner")
        self.assertEqual(self.coord.ack("m1", "p", 1, 1, at=6)["error"]["code"],
                         "stale_assignment")
        self.assertEqual(self.coord.poll("m2", "p", 1, at=6)["error"]["code"],
                         "stale_assignment")

    def test_force_revoke_entry(self):
        self._owned_partition()
        self.coord.poll("m1", "p", 1, at=2)  # 在途未确认，交接不会当场完成
        self.coord.upsert_member("m2", 1, ["v1"], 1, at=2)
        result = self.coord.force_revoke("p", at=3)
        self.assertTrue(result["ok"])
        self.assertEqual(result["from_member"], "m1")
        self.assertEqual(result["to_member"], "m2")
        self.assertEqual(self.coord.poll("m1", "p", 1, at=4)["error"]["code"], "revoked")
        self.coord.giveback("m1", "p", 1, at=5)
        self.assertEqual(self.coord.inspect("p")["owner"], "m2")
        # 强制撤回不会被后续重算撤销回原成员
        self.coord.rebalance(at=6)
        self.assertEqual(self.coord.inspect("p")["owner"], "m2")

    def test_member_boot_change_drops_old_ownership(self):
        self._owned_partition()
        self.coord.poll("m1", "p", 1, at=2)
        self.coord.upsert_member("m1", 2, ["v1"], 1, at=3)  # 新化身
        self.assertIsNone(self.coord.inspect("p")["owner"])
        self.coord.rebalance(at=4)
        view = self.coord.inspect("p")
        self.assertEqual((view["owner"], view["assignment"]), ("m1", 2))
        # 在途交付作废，从已确认位置重投
        fetched = self.coord.poll("m1", "p", 2, at=5)
        self.assertEqual([m["position"] for m in fetched["messages"]], [1])


class RetentionResetTests(unittest.TestCase):
    def setUp(self):
        self.coord = Coordinator(":memory:")

    def test_blocked_partition_does_not_block_others(self):
        self.coord.declare_partition("x", "v1", 1, 1, 5, at=0)
        self.coord.declare_partition("y", "v1", 1, 1, 5, at=0)
        self.coord.upsert_member("m1", 1, ["v1"], 2, at=0)
        self.coord.rebalance(at=1)
        self.coord.poll("m1", "x", 1, at=2)
        self.coord.poll("m1", "y", 1, at=2)
        self.coord.upsert_member("m2", 1, ["v1"], 2, at=3)
        self.coord.upsert_member("m1", 1, ["v1"], 0, at=3)
        self.coord.rebalance(at=4)
        # x 的保留点越过未确认位置 → 阻塞；y 正常交还并被新成员领取
        self.coord.declare_partition("x", "v1", 1, 3, 5, at=5)
        self.assertEqual(self.coord.inspect("x")["state"], "blocked_handoff")
        self.coord.giveback("m1", "y", 1, at=5)
        fetched = self.coord.poll("m2", "y", 2, at=6)
        self.assertTrue(fetched["ok"])
        # x 未重置不影响 y；x 重置后也恢复
        self.coord.reset("x", 1, 1, 2, at=7)
        fetched = self.coord.poll("m2", "x", 2, at=8)
        self.assertEqual([m["position"] for m in fetched["messages"]], [3])

    def test_active_partition_trim_blocks_poll_until_reset(self):
        self.coord.declare_partition("p", "v1", 1, 1, 5, at=0)
        self.coord.upsert_member("m1", 1, ["v1"], 1, at=0)
        self.coord.rebalance(at=1)
        self.coord.poll("m1", "p", 1, at=2)
        # 保留点越过未确认位置（无交接）：投递阻塞，组不能自行跳过
        self.coord.declare_partition("p", "v1", 1, 3, 5, at=3)
        result = self.coord.poll("m1", "p", 1, at=4)
        self.assertEqual(result["error"]["code"], "blocked")
        # 以当前 assignment 为核对标识重置
        bad = self.coord.reset("p", 9, 1, 2, at=5)
        self.assertEqual(bad["error"]["code"], "handoff_mismatch")
        good = self.coord.reset("p", 1, 1, 2, at=5)
        self.assertTrue(good["ok"])
        fetched = self.coord.poll("m1", "p", 1, at=6)
        self.assertEqual([m["position"] for m in fetched["messages"]], [3])

    def test_retention_rewind_and_format_change_rejected(self):
        self.coord.declare_partition("p", "v1", 1, 1, 5, at=0)
        self.assertEqual(
            self.coord.declare_partition("p", "v1", 1, 0, 5, at=1)["error"]["code"],
            "invalid_span")
        self.coord.declare_partition("p", "v1", 1, 3, 5, at=1)
        self.assertEqual(
            self.coord.declare_partition("p", "v1", 1, 2, 5, at=2)["error"]["code"],
            "retention_rewind")
        self.assertEqual(
            self.coord.declare_partition("p", "v2", 1, 3, 5, at=2)["error"]["code"],
            "format_immutable")


class PersistenceTests(unittest.TestCase):
    def test_restart_keeps_pending_handoff_and_progress(self):
        with tempfile.TemporaryDirectory() as directory:
            store = str(Path(directory) / "state.json")
            coord = Coordinator(store)
            coord.declare_partition("p", "v1", 1, 1, 5, at=0)
            coord.upsert_member("m1", 1, ["v1"], 1, at=0)
            coord.rebalance(at=1)
            coord.poll("m1", "p", 1, at=2)
            coord.poll("m1", "p", 1, at=2)
            coord.ack("m1", "p", 1, 1, at=2)
            coord.upsert_member("m2", 1, ["v1"], 1, at=3)
            coord.upsert_member("m1", 1, ["v1"], 0, at=3)
            coord.rebalance(at=4)

            # 重启：新实例从存储恢复未决交接与进度
            revived = Coordinator(store)
            summary = revived.restart()
            pending = summary["groups"][0]["pending_handoffs"]
            self.assertEqual(len(pending), 1)
            self.assertEqual(pending[0]["handoff"], 1)
            self.assertEqual(summary["groups"][0]["committed"], {"p": 1})
            view = revived.inspect("p")
            self.assertEqual(view["pending_handoff"]["delivered_boundary"], 2)
            self.assertEqual(view["committed"], 1)

            # 恢复后交接照常完成
            result = revived.ack("m1", "p", 1, 2, at=5)
            self.assertEqual(result["handoff_completed"], "drained")
            fetched = revived.poll("m2", "p", 2, at=6)
            self.assertEqual([m["position"] for m in fetched["messages"]], [3])


if __name__ == "__main__":
    unittest.main()
