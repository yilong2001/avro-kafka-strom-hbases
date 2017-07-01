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
### 2、poll
- （1） 执行 KafkaConsumer.pollOnce , 直到 timeout （poll.timeout.ms）
- （2） 如果 poll 成功，异步发送 poll，下次直接从 buffer 读取记录（预读取）
### 3、pollOnce
- （1） 校验与 broker 的连接， 一直持续直到成功或异常；
- （2） 调用 ConsumerCoordinatorensure.ensureActiveGroup 确保 group is active；
- （3） updateFetchPositions， 更新 offset；
- （4） 执行 ConsumerNetworkClient.poll，会启动 fetch 线程，同步读取数据；
- （5） 返回数据结果

## ConsumerCoordinator.class
### 1、commitOffsetsSync
- （1） 校验与 broker 的连接， 一直持续直到成功或异常；
-- （1.1） 如果 consumer 没有与 broker 建立连接，则一直发 GroupCoordinatorRequest 消息给 borker；
-- （1.2） 保证 Metadata 一致：从 request 得到的 version，与 sender 线程更新的 version 一致；
-- （1.3） 处理 GroupCoordinatorResponse 时，会执行 heartbeatTask.reset()；
- （2）  发送 commit 消息，一直持续直到成功或异常；

## AbstractCoordinator.class
### 1、ensureActiveGroup
- （1） 校验与 broker 的连接， 一直持续直到成功或异常；
- （2） 发送 JoinGroupRequest，直到收到结果，或者抛出异常
- （3） 执行 ConsumerCoordinator.onJoinComplete ，通知 ConsumerRebalanceListener；

## ConsumerNetworkClient.class
### 1、poll
- （1） 重新发送所有请求；
- （2） 计算超时间隔，调用 NetworkClient.poll ；
- （3） 如果有 broker 连接失败，重发请求 ClientRequest ；
- （4） 执行 delayedTasks （ 包括 heartbeat task ）；
- （5） 丢弃失败的请求；

## 参数分析
### poll.timeout.ms 
- （1）一次 kafka consumer poll 的时间间隔；
- （2）说明：default 200

### offset.commit.period.ms 
- （1）执行 topic offset commit 的时间间隔；
- （2）如果太小，会减少 poll 拉取数据和处理数据的时间，会减少吞吐量；
- （3）如果太大，会减少 commit 次数，如果此时待 emit 的数据比较多，导致 heartbeat 超时的可能性增加；

### max.uncommitted.offsets
- （1）通过 emmit 发出 tuple，但没有收到 ack 的最大的 tuple 数量；
- （2）default 10000000；
- （3）1.0.2在KafkaSpout.Build 代码默认是10000，一定要修改！

### session.timeout.ms
- （1）超过这个时间间隔没有收到 heartbeat ，会触发 rebalance ；
- （2）default 10000
- （3）如果太小，会导致 rebalance 可能性增加；
- （4）如果太大，相当于没有心跳检测，在 consumer down 掉的情况下，无法及时进行 rebalance ；

### max.partition.fetch.bytes
- （1）kafka consumer 一次消费数据的最大字节数
- （2）default 1M
- （3）如果太大，一次获取的数据量比较多，如果大量数据在一段时间内收不到 ack ，会导致 无法进行 poll的操作，增加 heartbeat 超时的可能性；
- （4）如果太小，会频繁的 poll 数据（poll的时候，不能emit），会导致 network IO 时间增加，减少吞吐量；
- （5）如果太小，一次大批量的 fail ，会导致无法进行 commit offset， 就可能导致 heartbeat 的产生；

### TOPOLOGY_MESSAGE_TIMEOUT_SECS
- （1）任意一个tuple处理失败或者超时
- （2）如果太小，消息容易超时，从而导致错误的重发；
- （3）如果太大，消息失败，但是没有检测到超时，容易导致 无法commit 或者 无法 poll



