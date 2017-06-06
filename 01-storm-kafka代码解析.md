## 适用版本
- storm 1.0.2 - 1.1.0
- kafka 0.9.0.1 - latest
- hbase 1.2.0 - latest
- avro 1.7.7 - latest

## kafka-storm类结构

![image](https://github.com/yilong2001/avro-kafka-strom-hbases/blob/master/imgs/storm-kafka.png)

- kafka-strom类结构关系

## KafkaSpout.class
### 1、主循环 -- nextTuple
- 由3部分组成：
- commit、poll、emit
- 主要功能为：
- 更新 TopicPartition offset、 从 kafka broker 读取数据、 发送 tuple 到下级 bolt

- （1） commit 满足的条件（做一次 topic partition commit），两条同时满足
- （1.1） enable.auto.commit = false
- （1.2） offset.commit.period.ms 设定的时间间隔到期 (30000)

- （2） poll 满足的条件 （做一次 kafka consumer 的 poll ），两条同时满足
- （2.1） waitingToEmit = false（没有要发出去的 tuple），正好与（3）相反
- （2.2） numUncommittedOffsets < max.uncommitted.offsets（10,000,000）

- （3） emit满足的条件（waitingToEmit = true）
- （3.1） waitingToEmitList 不等于 null
- （3.2） waitingToEmitList.hasNext

### 2、执行kafka offset的commit功能 -- commitOffsetsForAckedTuples
- 如果满足 commit 条件(在 nextTuple)，执行此功能
- （1） 按照 TopicPartition 更新 nextCommitOffset 队列
- （2） 如果 nextCommitOffset 不等于 null，执行 KafkaConsumer.commitSync 动作

### 3、从kafka消费数据 -- pollKafkaBroker
- 如果满足 poll 条件（在nextTuple），执行 poll 动作
- （1） 以 poll.timeout.ms 为超时时间，执行 KafkaConsumer.poll 动作
- （2）获取到的记录，放到 waitingToEmit 队列中

### 4、发送数据到下一级bolt -- emit
- 如果满足 emit 条件（在nextTuple），执行具体的 emit 动作
- （1）调用 KafkaSpoutStreamsNamedTopics.emit 发送 tuple 到下一个 bolt；

## KafkaConsumer.class
### 1、commitSync
- （1） 调用acquire方法， 执行线程安全性检测，确保与初始化线程一致
- （2） 调用 ConsumerCoordinator.commitOffsetsSync 




