# kafka
kafka的优势

消息队列：解耦、缓冲、削峰、灵活性高、异步

基于发布/订阅模式 相比点对点（一个消息只能被一个消费者消费）处理的更快

发布/订阅模式也分为队列推数据和消费者拉取数据

队列推数据：可能产生推数据过快，消费者来不及消费的情况，崩掉的情况，也有可能存在资源浪费的情况

消费者主动拉取缺点：consumer不断向topic进行轮询，造成资源的浪费



同一个消费者组的不同消费者不能够消费同一个主题的同一个分区



zookeeper作用：存储kafka的集群信息，存储kafka消费者的消费位置信息 0.9版本之后记录在系统主题当中，不存在zookeeper是因为consumer在消费的同时，和集群交互同时还要和zookeeper交互，效率太低


kafka消息存储在磁盘，默认存7天

# kafka broker
分区的好处：

提高负载

增加并行度

# kafka生产者分区规则
指定分区

根据key的哈希值取余分区数

round-robin轮询


kafka 常用命令

查看topic

```
 bin/kafka-topics.sh  --zookeeper xpx101:2181/kafka --list
```

创建topic

```
bin/kafka-topics.sh --zookeeper xpx101:2181/kafka --create --replication-factor 3 --partitions 3 --topic first
```

# ISR
ISR队列的作用：因为kafka副本同步策略选择的是所有的副本全部同步，但是可能存在部分机器损坏导致阻塞，通过设置最大等待时间的参数（默认10秒），超过这个时间就剔除ISR队列

follower被踢出ISR之后如何重新加入：将自身高于HW的部分截取掉，当自己的LEO到达该分区的HW后重新加入ISR

# ack级别
0：直接发  最容易丢数据

1：leader接收完  较可能丢数据

-1：leader和follower（ISR队列里的follower）都接收完  保证不丢数据，可能数据重复

# 保证数据一致性
LEO：每个副本的最后一个offset

HW：ISR副本中最小的LEO

消费端一致性：comsumer只能消费到HW

存储端一致性：leader挂掉时，新leader以外的ISR队列成员将HW以外的截取掉，统一向新leader同步

# Excatly Once 精准一次性消费
Excatly Once：ack=-1+幂等性（开启参数即可）

幂等性原理：在produce初始化时会生成pid，这个生产者的消息会附带序列化信息，broker端会有一个<pid,partition,seq>的缓存，一个信息只有持久化一次

因为produce重启会生成新的pid，所以不支持跨会话跨分区的精准一次

# 消费者分区分配规则
消费者数量变化时触发

RoundRobin:轮询  所有主题+分区作为整体，消费者之间资源最多只差一个，无法满足一个消费者组只消费一个主题的需求

Range:范围 每个主题作为一个整体，可以指定消费者组消费指定分区，但是可能消费者资源分配不均

默认range

# Offset维护
由主题+分区+消费者组确定一个offset


代码测试

创建主题

```
bin/kafka-topics.sh --create --topic bigdata --zookeeper xpx101:2181/kafka --partitions 2 --replication-factor 2
```

启动生产者

```
bin/kafka-console-producer.sh --broker-list xpx101:9092 --topic bigdata
```

启动消费者

```
bin/kafka-console-consumer.sh --bootstrap-server xpx101:9092 --topic bigdata --consumer-property group.id=test1-group --from-beginning
```

查看消费者的offset信息

```
./kafka-consumer-groups.sh --bootstrap-server xpx101:9092 --describe --group test1-group
```

zookeeper查看,新版kafka无法查看到controller节点下的信息

```
ls /kafka
get /kafka/controller
```


# Kafka落盘仍高效的原因
分布式，有分区，并行度高

顺序写磁盘，减少寻址时间

使用了零拷贝技术，减少了传输过程，文件-操作系统-到用户层 简化为文件-操作系统-文件

# Controller作用
Kafka 集群中有一个 broker 会被选举为 Controller，负责管理集群broker的上下线，所有 topic 的分区副本分配和 leader选举

# kafka组件顺序
拦截器-序列化-分区器

# Producer事务
客户端提供唯一的事务ID（Transaction ID），将pid和事务ID绑定，重启之后根据Transaction ID获取pid,实现跨分区跨回话的精准一次

消费者事务则是消费一次

[>>返回首页](/)