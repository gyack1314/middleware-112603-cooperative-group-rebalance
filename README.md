# 订阅组的协作式分区再分配与扩容边界

分区内位置从 1 起，格式由 format 声明。每个组的 assignment 是该分区实际所有权的代次。

## 输入消息

每个 JSONL 文件是一段独立输入记录。每行包括整数秒点 at、消息名称 kind 与对象 record。at 代表中间件收到消息的时刻，同一秒内按行序提交。业务时间和对应回执引用写在 record 中。

- **partition**　发布 partition、format、priority、low 与 high。
- **member**　组中成员 id、boot、formats 与 slots，slots 为 0 表示退出。
- **rebalance**　触发重新协调。
- **poll**　指定成员申请从 assignment 所指位置领取。 测试记录每次有权领取时最多取一条。
- **ack**　成员确认此前实际领取的 through。
- **giveback**　成员确认不再保留所列分区的旧交付。
- **reset**　管理员允许明确 from/to 这段范围作为损失，并以当前 handoff 标识约束。
- **restart**　重启。
- **inspect**　按分区查。

## 示例

- changing-one-partition.jsonl
- trimmed-before-handoff.jsonl

## 读取演练资料

运行环境为 Python 3.11，只使用标准库。在根目录运行以下命令可核对文件格式及读取接口。

    python3 scenario_reader.py examples
    python3 -m unittest discover -v

资料读取器会按文件统计各类参与方消息的数量。演练记录只描述各参与方交给系统的输入。

## 协调器实现

`coordinator/` 是演练记录对应的消费组协调器，只用标准库：

- **core.py**　组内核心：成员能力（formats、slots）、分区分配、生效交接（handoff）、消费进度（committed）。
- **service.py**　多组管理与持久化：每次变更原子写入状态文件，重启后未决交接与分区进度原样恢复。
- **replay.py**　按 JSONL 演练记录逐条驱动协调器并打印可观察结果。
- **cli.py**　持续运行入口：建组、成员更新、领取、撤回、交还、重置与运营视图。
- **demo.py**　本地合成演练，覆盖领取/确认、最小移动、交还重投、保留点阻塞与重置、重启恢复、优先级抢占。

### 语义要点

- 分配同时满足资格与容量；无资格分区留空并给出原因（`no_eligible_member` / `capacity_exhausted`），不因均衡交给看不懂格式的成员。
- 重新协调是惰性的：成员或分区变化先登记，`rebalance` 才重算。仍合法且无需移动的分区继续投递；只有实际被撤回的分区停止新批次。容量不足时高优先级分区先落位，可抢占低优先级分区。
- 交接记录实际投递边界，再平衡与重启都不会遗忘。旧成员可在租约内确认已取批次（确认完毕即完成交接），或明确交还未处理部分；确认必须连续且不得超过实际投递边界，新成员只从已确认的下一项开始。
- 旧租期过期后迟到的确认整段拒绝，不得越过新成员已取得的批次；交接台账可按 `expired` / `giveback` / `drained` / `reset` 分别查询。
- 待交接时保留点越过未确认位置，交接进入阻塞，组不能自行跳过；管理员按精确损失范围 reset，并绑定本次交接编号。未重置的分区不阻塞其它分区。
- `inspect` 按分区给出实际拥有者、正在等待的确认或 reset、以及最近决策原因；`status` 给出组总览。

### 运行

    python3 -m coordinator.replay examples/changing-one-partition.jsonl
    python3 -m coordinator.replay examples/trimmed-before-handoff.jsonl
    python3 -m coordinator.demo
    python3 -m unittest discover -v

CLI 示例（状态落在 `--store` 文件中，进程重启即恢复）：

    python3 -m coordinator.cli --store /tmp/cg.json create-group
    python3 -m coordinator.cli --store /tmp/cg.json partition --name p --format v1 --priority 2 --low 1 --high 8
    python3 -m coordinator.cli --store /tmp/cg.json member --id old --boot 1 --formats v1,v2 --slots 2
    python3 -m coordinator.cli --store /tmp/cg.json rebalance
    python3 -m coordinator.cli --store /tmp/cg.json poll --member old --partition p --assignment 1
    python3 -m coordinator.cli --store /tmp/cg.json ack --member old --partition p --assignment 1 --through 1
    python3 -m coordinator.cli --store /tmp/cg.json giveback --member old --partition p --assignment 1
    python3 -m coordinator.cli --store /tmp/cg.json revoke --partition p
    python3 -m coordinator.cli --store /tmp/cg.json reset --partition p --handoff 1 --from 2 --to 3
    python3 -m coordinator.cli --store /tmp/cg.json inspect --partition p
    python3 -m coordinator.cli --store /tmp/cg.json handoffs --completed-by expired
