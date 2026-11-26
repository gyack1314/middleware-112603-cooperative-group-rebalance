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
