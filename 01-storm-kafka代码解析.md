##适用版本
- storm 1.0.2 - 1.1.0
- kafka 0.9.0.1 - latest
- hbase 1.2.0 - latest
- avro 1.7.7 - latest

##kafka-storm类结构

![image](https://github.com/yilong2001/avro-kafka-strom-hbases/blob/master/imgs/storm-kafka.png)

- kafka-strom类结构关系

## KafkaSpout.class
###1、主循环--nextTuple
- 由3部分组成：
- commit、poll、emit
- 主要功能为：
- 更新 TopicPartition offset、 从 kafka broker 读取数据、 发送 tuple 到下级 bolt

- （1） commit 满足的条件（做一次 topic partition commit），两条同时满足
- （1.1） enable.auto.commit = false
- （1.2） offset.commit.period.ms 设定的时间间隔到期 (30000)

- （2） poll 满足的条件 （做一次 kafka consumer 的 poll ），两条同时满足
- （2.1） waitingToEmit = false（没有要发出去的 tuple）
-  --> waitingToEmit = true，需要满足的条件
- （2.1.1） waitingToEmit 队列 不等于 null
- （2.1.2） waitingToEmit.hasNext，队列

- （2.2） numUncommittedOffsets < max.uncommitted.offsets（10,000,000）

- （3） emit满足的条件（waitingToEmit = true）
- （3.1） waitingToEmit 队列 不等于 null
- （3.2） waitingToEmit.hasNext，队列


