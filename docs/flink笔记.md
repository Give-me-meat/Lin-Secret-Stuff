# **flink**运行架构

![1660277392250](https://gitee.com/it-wont-work/typora-cloud-map-library/raw/master/img/1660277392250.png)

三个组件

- 客户端：负责**作业的提交**，将代码转化成数据流图，最后生成作业图
- jobManager:Flink 集群中任务管理和调度的核心，是控制应用执行的主进程
- TaskManager:工作进程,负责执行任务处理数据



JobManger 包含 3 个不同的组件

* JobMaster
  * 负责处理单独的作业（Job），JobMaster和具体的 Job 是一一对应
  * JobMaster 接收来自客户端的应用，接收到的信息包括jar包，数据流图（dataflow graph）和作业图（JobGraph）
  *  JobGraph 转换成一个物理层面的数据流图，也就是“执行图”，它包含了所有可以并发执行的任务，JobMaster会向ResourseManager发出请求，申请资源。获取到足够资源后，会将执行图分发到运行的TaskManager上
  * 负责中央协调操作，比如checkpoint
* ResourceManager （资源管理器）
  * 主要负责资源的分配和管理，在 Flink  集群中只有一个。所谓“资源”，主要是指TaskManager 的任务槽（task slots）。任务槽就是 Flink 集群中的资源调配单元，包含了机器用来执行计算的一组 CPU 和内存资源。每一个任务（Task）都需要分配到一个 slot 上执行。
* Dispatcher (分发器)
  * 主要负责提供一个 REST 接口，用来提交应用，并且负责为每一个新提交的作业启动一个新的 JobMaster 组件。Dispatcher 也会启动一个 Web UI，用来方便地展示和监控作业执行的信息。Dispatcher 在架构中并不是必需的，在不同的部署模式下可能会被忽略掉。



TaskManger

* TaskManager 是 Flink 中的工作进程，数据流的具体计算就是它来做的，所以也被称为“Worker”。Flink  集群中必须至少有一个 TaskManager；当然由于分布式计算的考虑，通常会有多个 TaskManager 运行，每一个 TaskManager 都包含了一定数量的任务槽（task slots）。Slot是资源调度的最小单位，slot 的数量限制了TaskManager 能够并行处理的任务数量。
* 启动之后，TaskManager 会向资源管理器注册它的 slots；收到资源管理器的指令后， TaskManager 就会将一个或者多个槽位提供给 JobMaster 调用，JobMaster 就可以分配任务来执行了
* 在执行过程中，TaskManager 可以缓冲数据，还可以跟其他运行同一应用的 TaskManager交换数据



## 作业提交流程

### 高层级抽象视角

![a1](https://gitee.com/it-wont-work/typora-cloud-map-library/raw/master/img/a1.png)

（1） 一般情况下，由客户端（App）通过分发器提供的 REST 接口，将作业提交给JobManager。

（2） 由分发器启动 JobMaster，并将作业（包含 JobGraph）提交给 JobMaster。

（3） JobMaster 将 JobGraph 解析为可执行的ExecutionGraph，得到所需的资源数量，然后向资源管理器请求资源（slots）。

（4） 资源管理器判断当前是否由足够的可用资源；如果没有，启动新的 TaskManager。

（5） TaskManager 启动之后，向ResourceManager 注册自己的可用任务槽（slots）。

（6） 资源管理器通知 TaskManager 为新的作业提供 slots。

（7） TaskManager 连接到对应的 JobMaster，提供 slots。

（8） JobMaster 将需要执行的任务分发给TaskManager。

（9）TaskManager 执行任务，互相之间可以交换数据



### Yarn Session提交流程

![1660279809736](https://gitee.com/it-wont-work/typora-cloud-map-library/raw/master/img/1660279809736.png)

（1） 客户端通过REST 接口，将作业提交给分发器。

（2） 分发器启动 JobMaster，并将作业（包含 JobGraph）提交给 JobMaster。

（3） JobMaster 向资源管理器请求资源（slots）。

（4） 资源管理器向 YARN 的资源管理器请求container 资源。

（5） YARN 启动新的TaskManager 容器。

（6） TaskManager 启动之后，向 Flink 的资源管理器注册自己的可用任务槽。

（7） 资源管理器通知 TaskManager 为新的作业提供 slots。

（8） TaskManager 连接到对应的 JobMaster，提供 slots。

（9）JobMaster 将需要执行的任务分发给TaskManager，执行任务



### YARN per job模式

![1660280176755](https://gitee.com/it-wont-work/typora-cloud-map-library/raw/master/img/1660280176755.png)

（1） 客户端将作业提交给 YARN 的资源管理器，这一步中会同时将 Flink 的 Jar 包和配置上传到 HDFS，以便后续启动 Flink 相关组件的容器。

（2） YARN 的资源管理器分配 Container 资源，启动 Flink JobManager，并将作业提交给JobMaster。这里省略了 Dispatcher 组件。

（3） JobMaster 向资源管理器请求资源（slots）。

（4） 资源管理器向 YARN 的资源管理器请求container 资源。

（5） YARN 启动新的TaskManager 容器。

（6） TaskManager 启动之后，向 Flink 的资源管理器注册自己的可用任务槽。

（7） 资源管理器通知TaskManager 为新的作业提供 slots。

（8） TaskManager 连接到对应的 JobMaster，提供 slots。

（9）JobMaster 将需要执行的任务分发给TaskManager，执行任务



## 数据流图（Dataflow Graph）

flink程序由三部分构成：Source、Transformation 和 Sink

* Source 表示“源算子”，负责读取数据源。
* Transformation 表示“转换算子”，利用各种算子进行处理加工。
*  Sink 表示“下沉算子”，负责数据的输出。

在运行时，Flink 程序会被映射成所有算子按照逻辑顺序连接在一起的一张图，这被称为“逻辑数据流”（logical dataflow），或者叫“数据流图”（dataflow graph） 



## 并行度（Parallelism）

一个特定算子的子任务（subtask）的个数被称之为其并行度（parallelism）



## 算子链

数据传输形式

*  一对一（One-to-one，forwarding）
  * 这种模式下，数据流维护着分区以及元素的顺序。比如图中的 source 和 map 算子，source 算子读取数据之后，可以直接发送给 map 算子做处理，它们之间不需要重新分区，也不需要调整数据的顺序。这就意味着 map 算子的子任务，看到的元素个数和顺序跟 source 算子的子任务产生的完全一样，保证着“一对一”的关系。map、filter、flatMap 等算子都是这种 one-to-one 的对应关系。
*  重分区（Redistributing）
  * 每一个算子的子任务，会根据数据传输的策略，把数据发送到不同的下游目标任务。例如， keyBy()是分组操作，本质上基于键（key）的哈希值（hashCode）进行了重分区；而当并行度改变时，比如从并行度为 2 的 window 算子，要传递到并行度为 1 的 Sink 算子，这时的数据传输方式是再平（rebalance），会把数据均匀地向下游子任务分发出去。这些传输方式都会引起重分区（redistribute）的过程，这一过程类似于 Spark 中的 shuffle。



算子链合并条件：两个算子是One-to-one的传输关系且并行度一致 



## 执行图

（1）逻辑流图（StreamGraph）

这是根据用户通过 DataStream API 编写的代码生成的最初的 DAG 图，用来表示程序的拓扑结构。这一步一般在客户端完成。

我们可以看到，逻辑流图中的节点，完全对应着代码中的四步算子操作：

源算子 Source（socketTextStream()）→扁平映射算子 Flat Map(flatMap()) →分组聚合算子

Keyed Aggregation(keyBy/sum()) →输出算子 Sink(print())。

（2）作业图（JobGraph）

StreamGraph **经过优化**后生成的就是作业图（JobGraph），这是提交给 JobManager  的数据结构，确定了当前作业中所有任务的划分。主要的优化为: **将多个符合条件的节点链接在一起合并成一个任务节点，形成算子链**，这样可以减少数据交换的消耗。JobGraph 一般也是在客户端生成的，在作业提交时传递给 JobMaster。

在图 4-12 中，分组聚合算子（Keyed Aggregation）和输出算子 Sink(print)并行度都为 2， 而且是一对一的关系，满足算子链的要求，所以会合并在一起，成为一个任务节点。

（3）执行图（ExecutionGraph）

**JobMaster** 收到 JobGraph 后，会根据它来**生成执行图**（ExecutionGraph）。ExecutionGraph

是 JobGraph 的并行化版本，是调度层最核心的数据结构。

从图 4-12 中可以看到，与 JobGraph 最大的区别就是**按照并行度对并行子任务进行了拆分， 并明确了任务间数据传输的方式。**

（4） 物理图（Physical Graph）

JobMaster 生成执行图后， 会将它分发给TaskManager；各个 TaskManager 会根据执行图部署任务，最终的物理执行过程也会形成一张“图”，一般就叫作物理图（Physical  Graph）。这只是具体执行层面的图，并不是一个具体的数据结构。

对应在上图 4-12 中，物理图主要就是在执行图的基础上，**进一步确定数据存放的位置和收发的具体方式**。有了物理图，TaskManager 就可以对传递来的数据进行处理计算了。

所以我们可以看到，程序里定义了四个算子操作：源（Source）->转换（flatMap）->分组聚合（keyBy/sum）->输出（print）；合并算子链进行优化之后，就只有三个任务节点了；再考虑并行度后，一共有 5 个并行子任务，最终需要 5 个线程来执行。



## 任务（Tasks）和任务槽（Task Slots）

Flink 中每一个 worker(也就是 TaskManager)都是一个 JVM 进程



Flink 是允许子任务共享 slot 的。 只要属于同一个作业，那么对于不同任务节点的并行子任务，就可以放到同一个 slot 上执行。



## Batch模式

代码

```
bin/flink run -Dexecution.runtime-mode=BATCH ...
```



# DataStream API



## lambda表达式

实现单一抽象方法的接口可用

单一抽象方法（Single Abstract Method，SAM）接口

flatMap 使用 Lambda 表达式，必须通过 returns 明确声明返回类型



## 常用算子

### flatMap 

可以认为是“扁平化”（flatten）和“映射”（map）两步操作的结合，也就是先按照某种规则对数据进行打散拆分，再对拆分后的元素做转换处理

```java
stream.flatMap((Event value ,Collector<String> out) ->{
            if (value.user.equals("Mary")) {
                out.collect(value.user);
            } else if (value.user.equals("Bob")) {
                out.collect(value.user);
                out.collect(value.url);
            }
        })
```



### filter

过滤

```java
stream.filter(data -> data. user.equals("Bob")).print("lamada");
```



### keyBy 

对于 Flink 而言，DataStream 是没有直接进行聚合的API 的。因为我们对海量数据做聚合肯定要进行分区并行处理，这样才能提高效率。所以在 Flink 中，要做聚合，需要先进行分区； 这个操作就是通过 keyBy 来完成的。

keyBy 是聚合前必须要用到的一个算子。keyBy 通过指定键（key），可以将一条流从逻辑上划分成不同的分区（partitions）。这里所说的分区，其实就是并行处理的子任务，也就对应着任务槽（task slot）。



### 富函数类

富函数类可以获取运行环境的上下文

Rich Function 有生命周期的概念。典型的生命周期方法有：

* open()方法，是 Rich Function 的初始化方法，也就是会开启一个算子的生命周期。当一个算子的实际工作方法例如 map()或者 filter()方法被调用之前，open()会首先被调用。所以像文件 IO 的创建，数据库连接的创建，配置文件的读取等等这样一次性的工作，都适合在 open()方法中完成。。
* close()方法，是生命周期中的最后一个调用的方法，类似于解构方法。一般用来做一 些清理工作。



## 物理分区

常见的物理分区策略有随机分配（Random）、轮询分配（Round-Robin）、重缩放（Rescale）和广播（Broadcast）

### 随机分区

```java
stream.shuffle().print("shuffle").setParallelism(4);
```



### 轮询分区

```java
stream.rebalance().print("rebalance").setParallelism(4);
```



### 自定义分区

```java
 env.fromElements(1, 2, 3, 4, 5, 6, 7, 8)
                .partitionCustom(new Partitioner<Integer>() {
                    @Override
                    public int partition(Integer key, int numPartitions) {
                        return key % 2;
                    }
                }, new KeySelector<Integer, Integer>() {
                    @Override
                    public Integer getKey(Integer value) throws Exception {
                        return value;
                    }
                })
                .print().setParallelism(2);
```

 

# Flink中的时间和窗口



## 水位线

在事件时间语义下，我们不依赖系统时间，而是基于数据自带的时间戳去定义了一个时钟， 用来表示当前时间的进展。于是每个并行子任务都会有一个自己的逻辑时钟，它的前进是靠数据的时间戳来驱动的。

* 水位线是插入到数据流中的一个标记，可以认为是一个特殊的数据
* 水位线主要的内容是一个时间戳，用来表示当前事件时间的进展
* 水位线是基于数据的时间戳生成的
*  水位线的时间戳必须单调递增，以确保任务的事件时间时钟一直向前推进
*  水位线可以通过设置延迟，来保证正确处理乱序数据
*  一个水位线 Watermark(t)，表示在当前流中事件时间已经达到了时间戳 t, 这代表 t 之前的所有数据都到齐了，之后流中不会出现时间戳 t’ ≤ t 的数据

水位线是 Flink 流处理中保证结果正确性的核心机制，它往往会跟窗口一起配合，完成对乱序数据的正确处理。关于这部分内容，我们会稍后进一步展开讲解。



生成水位线的方法

assignTimestampsAndWatermarks()方法需要传入一个 WatermarkStrategy 作为参数，这就是 所 谓 的 “ 水 位 线 生 成 策 略 ” 。 WatermarkStrategy 中 包 含 了 一 个 “ **时 间 戳 分 配器**”TimestampAssigner 和一个“**水位线生成器”**WatermarkGenerator



代码实现

* TimestampAssigner：主要负责从流中数据元素的某个字段中提取时间戳，并分配给元素。时间戳的分配是生成水位线的基础。
*  WatermarkGenerator： 主要负责按照既定的方式， 基于时间戳生成水位线。在WatermarkGenerator 接口中，主要又有两个方法：onEvent()和 onPeriodicEmit()。指定处理的类型（有序，无序）以及参数（无序的延迟时间）
*  onEvent：每个事件（数据）到来都会调用的方法，它的参数有当前事件、时间戳， 以及允许发出水位线的一个 WatermarkOutput，可以基于事件做各种操作
*    onPeriodicEmit：周期性调用的方法，可以由 WatermarkOutput 发出水位线。周期时间为处理时间，可以调用环境配置的.setAutoWatermarkInterval()方法来设置，默认为200ms。



## 窗口

![1660317944622](https://gitee.com/it-wont-work/typora-cloud-map-library/raw/master/img/1660317944622.png)

Flink 中的窗口“存储桶”示意



### 窗口分类

* 按照驱动类型分类

  * 时间窗口
  * 技数窗口

* 按照窗口分配数据的规则分类

  * 滚动窗口:滚动窗口可以基于时间定义，也可以基于数据个数定义；需要的参数只有一个，就是窗口的大小（window size）

    * 特点：数据无重叠，会话窗口的长度固定

  * 滑动窗口：与滚动窗口类似，滑动窗口的大小也是固定的。区别在于，窗口之间并不是首尾相接的， 而是可以“错开”一定的位置。如果看作一个窗口的运动，那么就像是向前小步“滑动”一样。

    * 特点：会话窗口的长度固定，数据可能有重叠，频率
    * 窗口大小代表数据收集范围
    * 滑动大小代表数据获取频率

  * 会话窗口： Flink 底层，对会话窗口的处理会比较特殊：每来一个新的数据，都会创建一个新的会话窗口；然后判断已有窗口之间的距离，如果小于给定的 size，就对它们进行合并（merge） 操作。在Window 算子中，对会话窗口会有单独的处理逻辑。

    我们可以看到，与前两种窗口不同，会话窗口的长度不固定，起始和结束时间也是不确定的，各个分区之间窗口没有任何关联。如图 6-19 所示，会话窗口之间一定是不会重叠的，而且会留有至少为 size 的间隔（session gap）

    * 特点：会话窗口的长度不固定，起始和结束时间也是不确定的，只确定超时数据

  * 全局窗口：把相同 key 的所有数据都分配到同一个窗口中；说直白一点，就跟没分窗口一样无界流的数据永无止尽，所以这种窗口也没有结束的时候，默认是不会做触发计算的。如果希望它能对数据进行计算处理， 还需要自定义“触发器”（Trigger）



代码

```java
stream.keyBy(<key selector>)
.window(<window assigner>) //窗口分配器
.aggregate(<window function>) //窗口函数
```



## 窗口分配器

指定窗口类型和相关参数（窗口大小）





### 窗口函数

定义了窗口分配器，我们只是知道了数据属于哪个窗口，可以将数据收集起来了；至于收集起来到底要做什么，其实还完全没有头绪。所以在窗口分配器之后，必须再接上一个定义窗口如何进行计算的操作，这就是所谓的“窗口函数”（window functions）



分类

## 增量聚合函数（incremental aggregation functions）

窗口将数据收集起来，最基本的处理操作当然就是进行聚合。窗口对无限流的切分，可以看作得到了一个有界数据集。如果我们等到所有数据都收集齐，在窗口到了结束时间要输出结果的一瞬间再去进行聚合，显然就不够高效了——这相当于真的在用批处理的思路来做实时流处理。

为了提高实时性，我们可以再次将流处理的思路发扬光大：就像 DataStream 的简单聚合一样，每来一条数据就立即进行计算，中间只要保持一个简单的聚合状态就可以了；区别只是在于不立即输出结果，而是要等到窗口结束时间。等到窗口到了结束时间需要输出计算结果的时候，我们只需要拿出之前聚合的状态直接输出，这无疑就大大提高了程序运行的效率和实时性

#### 归约函数：ReduceFunction

#### 聚合函数：AggregateFunction

重写的方法

![1660392510635](https://gitee.com/it-wont-work/typora-cloud-map-library/raw/master/img/1660392510635.png)



### 全窗口函数（full window functions）

窗口操作中的另一大类就是全窗口函数。与增量聚合函数不同，全窗口函数需要先收集窗口中的数据，并在内部缓存起来，等到窗口要输出结果的时候再取出数据进行计算。

很明显，这就是典型的批处理思路了——先攒数据，等一批都到齐了再正式启动处理流程。这样做毫无疑问是低效的：因为窗口全部的计算任务都积压在了要输出结果的那一瞬间，而在之前收集数据的漫长过程中却无所事事。这就好比平时不用功，到考试之前通宵抱佛脚，肯定不如把工夫花在日常积累上。

#### 窗口函数（windowFunction）

#### 处理窗口函数（ProcessWindowFunction）



经常使用AggregateFunction + ProcessWindowFunction 配合使用，既能流式处理，又可以得到上下文的信息

代码

```java
public class linProcessTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

        stream.print("data");



        stream.keyBy(data -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(2)))
                .aggregate(new AvgPv(),new ProFunc())
                .print();



        env.execute();
    }



    public static  class ProFunc extends ProcessWindowFunction<Long,String,Boolean,TimeWindow>{
        @Override
        public void process(Boolean aBoolean, Context context, Iterable<Long> iterable, Collector<String> collector) throws Exception {
            long start = context.window().getStart();
            long end = context.window().getEnd();
            Timestamp startts = new Timestamp(start);
            Timestamp endts = new Timestamp(end);
            Long uv = iterable.iterator().next();
            collector.collect("窗口" + startts.toString() + "————" + endts.toString() + "UV值为" + uv);
        }
    }

    public static class AvgPv implements AggregateFunction<Event, HashSet<String>, Long> {

        @Override
        public HashSet<String> createAccumulator() {
            return new HashSet<String>();
        }

        @Override
        public HashSet<String> add(Event event, HashSet<String> strings) {
            strings.add(event.user);
            return strings;
        }

        @Override
        public Long getResult(HashSet<String> strings) {
            return (long)strings.size();
        }

        @Override
        public HashSet<String> merge(HashSet<String> strings, HashSet<String> acc1) {
            return null;
        }
    }



}
```



## 迟到数据

对于乱序流，水位线本身就可以设置一个延迟时间；而做窗口计算时，我们又可以设置窗口的允许延迟时间；另外窗口还有将迟到数据输出到测输出流的用法



例子：如窗口大小10s，水位线延迟2s，窗口延迟1s

0-10s秒窗口  接收到12s数据的时候开始计算 此时水位线为0

0-10秒窗口    接收到72秒数据的时候关闭窗口 此时水位线为70

此后，接收到0-10s的数据放入侧输出流



# 处理函数

处理函数提供了一个“定时服务”（TimerService），我们可以通过它访问流中的事件（event）、时间戳（timestamp）、水位线（watermark），甚至可以注册“定时事件”。而且处理函数继承了 AbstractRichFunction 抽象类，所以拥有富函数类的所有特性，同样可以访问状态（state）和其他运行时信息。此外，处理函数还可以直接将数据输出到侧输出流（side output）中。所以，处理函数是最为灵活的处理方法，可以实现各种自定义的业务逻辑；同时也是整个 DataStream API 的底层基础



常用处理函数

（1） ProcessFunction

最基本的处理函数，基于DataStream 直接调用.process()时作为参数传入。

（2） KeyedProcessFunction

对流按键分区后的处理函数，基于 KeyedStream 调用.process()时作为参数传入。要想使用定时器，比如基于KeyedStream。

（3） ProcessWindowFunction

开窗之后的处理函数，也是全窗口函数的代表。基于WindowedStream 调用.process()时作为参数传入。

（4） CoProcessFunction )合并（connect）两条流之后的处理函数，基于 ConnectedStreams 调用.process()时作为参数传入。



KeyedProcessFunction可以实现定时器的功能

processElement()

![1660451216510](https://gitee.com/it-wont-work/typora-cloud-map-library/raw/master/img/1660451216510.png)

用于“处理元素”，定义了处理的核心逻辑。这个方法对于流中的每个元素都会调用一次， 参数包括三个：输入数据值 value，上下文 ctx，以及“收集器”（Collector）out。方法没有返回值，处理之后的输出数据是通过收集器 out 来定义的

Ontimer方法

用于定义定时触发的操作，这是一个非常强大、也非常有趣的功能。这个方法只有在注册好的定时器触发的时候才会调用，而定时器是通过“定时服务”TimerService 来注册的。打个比方，注册定时器（timer）就是设了一个闹钟，到了设定时间就会响；而.onTimer()中定义的， 就是闹钟响的时候要做的事。所以它本质上是一个基于时间的“回调”（callback）方法，通过时间的进展来触发；在事件时间语义下就是由水位线（watermark）来触发了



代码（处理时间）

```java
public class ProcessingTimeTimerTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 处理时间语义，不需要分配时间戳和watermark
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource());

        // 要用定时器，必须基于KeyedStream
        stream.keyBy(data -> true)
                .process(new KeyedProcessFunction<Boolean, Event, String>() {
                    @Override
                    public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                        Long currTs = ctx.timerService().currentProcessingTime();
                        out.collect("数据到达，到达时间：" + new Timestamp(currTs));
                        // 注册一个10秒后的定时器
                        ctx.timerService().registerProcessingTimeTimer(currTs + 10 * 1000L);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect("定时器触发，触发时间：" + new Timestamp(timestamp));
                    }
                })
                .print();

        env.execute();
    }
}
```



事件时间

```java
public class EventTimeTimerTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new CustomSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

        // 基于KeyedStream定义事件时间定时器
        stream.keyBy(data -> true)
                .process(new KeyedProcessFunction<Boolean, Event, String>() {
                    @Override
                    public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                        out.collect("数据到达，时间戳为：" + ctx.timestamp());
                        out.collect("数据到达，水位线为：" + ctx.timerService().currentWatermark() + "\n -------分割线-------");
                        // 注册一个10秒后的定时器
                        ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 10 * 1000L);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect("定时器触发，触发时间：" + timestamp);
                    }
                })
                .print();

        env.execute();
    }

    // 自定义测试数据源
    public static class CustomSource implements SourceFunction<Event> {
        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            // 直接发出测试数据
            ctx.collect(new Event("Mary", "./home", 1000L));
            // 为了更加明显，中间停顿5秒钟
            Thread.sleep(5000L);

            // 发出10秒后的数据
            ctx.collect(new Event("Mary", "./home", 11000L));
            Thread.sleep(5000L);

            // 发出10秒+1ms后的数据
            ctx.collect(new Event("Alice", "./cart", 11001L));
            Thread.sleep(5000L);
        }

        @Override
        public void cancel() { }
    }
}
```



# 多流转换

## 分流

### 侧输出流

![1660407610791](https://gitee.com/it-wont-work/typora-cloud-map-library/raw/master/img/1660407610791.png)



## 合流

### union

联合操作要求必须流中的数据类型必须相同

```java
stream1.union(stream2)
stream1.union(stream2, stream3, ...)
```

水位线取较小水位线的流



### connect

ConnectedStreams(连接流)

![1660408790463](https://gitee.com/it-wont-work/typora-cloud-map-library/raw/master/img/1660408790463.png)



根据key connect

```java
 appStream.connect(thirdpartStream)
                .keyBy(data -> data.f0, data -> data.f0)
                .process(new OrderMatchResult())
                .print();
```



## 窗口连接（Window Join）

```java
stream1.join(stream2)

.where(<KeySelector>)

.equalTo(<KeySelector>)

.window(<WindowAssigner>)

.apply(<JoinFunction>)
```

特点：开窗



## 间隔连接（Interval Join）

间隔联结具体的定义方式是，我们给定两个时间点，分别叫作间隔的“上界”（upperBound）和“下界”（lowerBound）；于是对于一条流（不妨叫作 A）中的任意一个数据元素 a，就可以开辟一段时间间隔：[a.timestamp + lowerBound, a.timestamp + upperBound],即以 a 的时间戳为中心，下至下界点、上至上界点的一个闭区间：我们就把这段时间作为可以匹配另一条流数据的“窗口”范围。所以对于另一条流（不妨叫B）中的数据元素 b，如果它的时间戳落在了这个区间范围内，a 和b 就可以成功配对，进而进行计算输出结果。所以匹配的条件为：

a.timestamp + lowerBound <= b.timestamp <= a.timestamp + upperBound

```java
stream1

.keyBy(<KeySelector>)

.intervalJoin(stream2.keyBy(<KeySelector>))

.between(Time.milliseconds(-2), Time.milliseconds(1))

.process (new ProcessJoinFunction<Integer, Integer, String(){ @Override
public void processElement(Integer left, Integer right, Context ctx, Collector<String> out) {
out.collect(left + "," + right);

}

});
```



![1660455713407](https://gitee.com/it-wont-work/typora-cloud-map-library/raw/master/img/1660455713407.png)

特点：无需开窗



## 窗口同组连接（Window CoGroup）

除窗口联结和间隔联结之外，Flink 还提供了一个“窗口同组联结”（window coGroup）操作。它的用法跟window join 非常类似，也是将两条流合并之后开窗处理匹配的元素，调用时只需要将.join()换为.coGroup()就可以了

```java
stream1.coGroup(stream2)
.where(<KeySelector>)
.equalTo(<KeySelector>)
.window(TumblingEventTimeWindows.of(Time.hours(1)))
.apply(<CoGroupFunction>)
```

FlatJoinFunction、JoinFunction和ProcessJoinFunction的区别

![1660456702109](https://gitee.com/it-wont-work/typora-cloud-map-library/raw/master/img/1660456702109.png)

![1660456733951](https://gitee.com/it-wont-work/typora-cloud-map-library/raw/master/img/1660456733951.png)

![1660457038155](https://gitee.com/it-wont-work/typora-cloud-map-library/raw/master/img/1660457038155.png)

![1660457228840](https://gitee.com/it-wont-work/typora-cloud-map-library/raw/master/img/1660457228840.png)

FlatJoinFunction相比JoinFunction没有指定返回类型，使用收集器，更灵活，ProcessJoinFunction在此基础上多了上下文信息

cogroup 调用的是集合，可以实现window join的效果，即内连接

windowjoin只能实现内连接，cogroup还可以实现外连接，更加通用，底层windowjoin就是通过cogroup实现



# 状态编程

状态：可以理解为历史数据

## 状态管理难点

- 状态的访问权限：一个分区上可能会有多个key,他们可能会同时访问和改变本地变量，所以针对不同的key，要对状态加以区分，状态不只是单纯的本地变量
- 容错性：状态只保存在内存中显然是不够稳定的，我们需要将它持久化保存，做一个备份；在发生故障后可以从这个备份中恢复状态。
- 分布式应用的横向扩展性：比如处理的数据量增大时，我们应该相应地对计算资源扩容，调大并行度。这时就涉及到了状态的重组调整

## 状态分类

### 托管状态（Managed State）和原始状态（Raw State）

托管状态（Managed State）和原始状态（Raw State）。托管状态就是由 Flink 统一管理的，状态的存储访问、故障恢复和重组等一系列问题都由 Flink 实现，我们只要调接口就可以；而原始状态则是自定义的，相当于就是开辟了一块内存，需要我们自己管理，实现状态的序列化和故障恢复

托管状态是由 Flink 的运行时（Runtime）来托管的；在配置容错机制后，状态会自动持久化保存，并在发生故障时自动恢复。当应用发生横向扩展时，状态也会自动地重组分配到所有的子任务实例上。对于具体的状态内容，Flink  也提供了值状态（ValueState）、列表状态（ListState）、映射状态（MapState）、聚合状态（AggregateState）等多种结构，内部支持各种数据类型。聚合、窗口等算子中内置的状态，就都是托管状态；我们也可以在富函数类（RichFunction）中通过上下文来自定义状态，这些也都是托管状态

算子状态（Operator State）和按键分区状态（Keyed State）

### 算子状态（Operator State）

状态作用范围限定为当前的算子任务实例，也就是只对当前并行子任务实例有效。这就意味着对于一个并行子任务，占据了一个“分区”，它所处理的所有数据都会访问到相同的状态， 状态对于同一任务而言是共享的，如图 9-3 所示。



![1660460126983](C:\Users\Lin\AppData\Roaming\Typora\typora-user-images\1660460126983.png)

 

​								图 9-3 算子状态（Operator State）

算子状态可以用在所有算子上，使用的时候其实就跟一个本地变量没什么区别——因为本地变量的作用域也是当前任务实例。在使用时，我们还需进一步实现CheckpointedFunction 接口



### 按键分区状态（Keyed State）

状态是根据输入流中定义的键（key）来维护和访问的，所以只能定义在按键分区流

（KeyedStream）中，也就 keyBy 之后才可以使用，如图 9-4 所示。

![1660460219119](C:\Users\Lin\AppData\Roaming\Typora\typora-user-images\1660460219119.png)

​								图 9-4 按键分区状态（Keyed State） 

按键分区状态应用非常广泛。之前讲到的**聚合算子必须在 keyBy 之后才能使用，就是因为聚合的结果是以Keyed State 的形式保存的**。另外，也可以通过富函数类（Rich Function） 来自定义Keyed State，所以只要提供了富函数类接口的算子，也都可以使用 Keyed State。

所以即使是map、filter 这样无状态的基本转换算子，我们也可以通过富函数类给它们“追加”Keyed State，或者实现 CheckpointedFunction 接口来定义Operator State；从这个角度讲， Flink 中所有的算子都可以是有状态的，不愧是“有状态的流处理”。

无论是 Keyed State 还是 Operator State，它们都是在本地实例上维护的，也就是说每个并行子任务维护着对应的状态，算子的子任务之间状态不共享。



 Flink 需要对 Keyed State 进行一些特殊优化。在底层，Keyed State 类似于一个分布式的映射（map）数据结构，所有的状态会根据 key 保存成键值对（key-value）的形式。这样当一条数据到来时，任务就会自动将状态的访问范围限定为当前数据的key，从 map 存储中读取出对应的状态值。所以具有相同 key 的所有数据都会到访问相同的状态，而不同 key 的状态之间是彼此隔离的



## 代码示例

指定状态分配器、状态名称、类型

![1660476911262](https://gitee.com/it-wont-work/typora-cloud-map-library/raw/master/img/1660476911262.png)

![1660476893521](https://gitee.com/it-wont-work/typora-cloud-map-library/raw/master/img/1660476893521.png)



### ListState

使用场景：

TOPN  将每个key的聚合结果放入list当中进行排序

全外连接 将两个流的每个数据放入一个集合状态中

### MapState

模拟滚动窗口的实现 根据时间戳计算出所属窗口，然后将数据放入到对应窗口的mapstate当中





## 状态生存时间TTL

一个优化的思路是直接在代码中调用.clear()方法去清除状态，但是有时候我们的逻辑要求不能直接清除。这时就需要配置一个状态的“生存时间”（time-to-live，TTL），当状态在内存中存在的时间超出这个值时，就将它清除

![1660505101452](https://gitee.com/it-wont-work/typora-cloud-map-library/raw/master/img/1660505101452.png)





## 算子状态

算子状态（Operator State）就是一个算子并行实例上定义的状态，作用范围被限定为当前算子任务。算子状态跟数据的 key 无关，所以不同 key 的数据只要被分发到同一个并行子任务， 就会访问到同一个Operator State。

算子状态的实际应用场景不如Keyed State 多，一般用在 Source 或 Sink 等与外部系统连接的算子上，或者完全没有 key 定义的场景

算子状态也支持不同的结构类型，主要有三种：ListState、UnionListState 和 BroadcastState



数据分区发生变化，带来的问题就是，怎么保证原先的状态跟故障恢复后数据的对应关系呢？

对于Keyed State 这个问题很好解决：状态都是跟 key 相关的，而相同key 的数据不管发往哪个分区，总是会全部进入一个分区的；于是只要将状态也按照 key 的哈希值计算出对应的分区，进行重组分配就可以了。恢复状态后继续处理数据，就总能按照 key 找到对应之前的状态，就保证了结果的一致性。所以 Flink 对Keyed State 进行了非常完善的包装，我们不需实现任何接口就可以直接使用。

而对于 Operator State 来说就会有所不同。因为不存在 key，所有数据发往哪个分区是不可预测的；也就是说，当发生故障重启之后，我们不能保证某个数据跟之前一样，进入到同一个并行子任务、访问同一个状态。所以 Flink 无法直接判断该怎样保存和恢复状态，而是提供了接口，让我们根据业务需求自行设计状态的快照保存（snapshot）和恢复（restore）逻辑。

CheckpointedFunction 接口

在 Flink  中，对状态进行持久化保存的快照机制叫作“检查点”（Checkpoint）。于是使用算子状态时，就需要对检查点的相关操作进行定义，实现一个 CheckpointedFunction 接口。

CheckpointedFunction 接口在源码中定义如下：

```java
public interface CheckpointedFunction {
    //保存状态快照到检查点时，调用这个方法
    void snapshotState(FunctionSnapshotContext var1) throws Exception;
	//初始化状态时调用这个方法，也会在恢复状态时使用
    void initializeState(FunctionInitializationContext var1) throws Exception;
}

```



```java
public class BufferingSinkExample {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(10000L);
//        env.setStateBackend(new EmbeddedRocksDBStateBackend());

//        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage(""));

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setMinPauseBetweenCheckpoints(500);
        checkpointConfig.setCheckpointTimeout(60000);
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        checkpointConfig.enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        checkpointConfig.enableUnalignedCheckpoints();


        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );

        stream.print("input");

        // 批量缓存输出
        stream.addSink(new BufferingSink(10));

        env.execute();
    }

    public static class BufferingSink implements SinkFunction<Event>, CheckpointedFunction {
        private final int threshold;
        private transient ListState<Event> checkpointedState;
        private List<Event> bufferedElements;

        public BufferingSink(int threshold) {
            this.threshold = threshold;
            this.bufferedElements = new ArrayList<>();
        }

        @Override
        public void invoke(Event value, Context context) throws Exception {
            bufferedElements.add(value);
            if (bufferedElements.size() == threshold) {
                for (Event element: bufferedElements) {
                    // 输出到外部系统，这里用控制台打印模拟
                    System.out.println(element);
                }
                System.out.println("==========输出完毕=========");
                bufferedElements.clear();
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            checkpointedState.clear();
            // 把当前局部变量中的所有元素写入到检查点中
            for (Event element : bufferedElements) {
                checkpointedState.add(element);
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            ListStateDescriptor<Event> descriptor = new ListStateDescriptor<>(
                    "buffered-elements",
                    Types.POJO(Event.class));

            checkpointedState = context.getOperatorStateStore().getListState(descriptor);

            // 如果是从故障中恢复，就将ListState中的所有元素添加到局部变量中
            if (context.isRestored()) {
                for (Event element : checkpointedState.get()) {
                    bufferedElements.add(element);
                }
            }
        }
    }
}
```



## 广播流

在代码上，可以直接调用 DataStream  的.broadcast()方法，传入一个“映射状态描述器”

（MapStateDescriptor）说明状态的名称和类型，就可以得到一个“广播流”（BroadcastStream）；进而将要处理的数据流与这条广播流进行连接（ connect ）， 就会得到“ 广播连接流”（BroadcastConnectedStream）。注意广播状态只能用在广播连接流中。



代码示例：用于匹配用户的行为模式，把模式信息放入广播流中

```java

public class BroadcastStateExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取用户行为事件流
        DataStreamSource<Action> actionStream = env.fromElements(
                new Action("Alice", "login"),
                new Action("Alice", "pay"),
                new Action("Bob", "login"),
                new Action("Bob", "buy")
        );

        // 定义行为模式流，代表了要检测的标准
        DataStreamSource<Pattern> patternStream = env
                .fromElements(
                        new Pattern("login", "pay"),
                        new Pattern("login", "buy")
                );

        // 定义广播状态的描述器，创建广播流
        MapStateDescriptor<Void, Pattern> bcStateDescriptor = new MapStateDescriptor<>(
                "patterns", Types.VOID, Types.POJO(Pattern.class));
        BroadcastStream<Pattern> bcPatterns = patternStream.broadcast(bcStateDescriptor);

        // 将事件流和广播流连接起来，进行处理
        DataStream<Tuple2<String, Pattern>> matches = actionStream
                .keyBy(data -> data.userId)
                .connect(bcPatterns)
                .process(new PatternEvaluator());

        matches.print();

        env.execute();
    }

    public static class PatternEvaluator
            extends KeyedBroadcastProcessFunction<String, Action, Pattern, Tuple2<String, Pattern>> {

        // 定义一个值状态，保存上一次用户行为
        ValueState<String> prevActionState;

        @Override
        public void open(Configuration conf) {
            prevActionState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("lastAction", Types.STRING));
        }

        @Override
        public void processBroadcastElement(
                Pattern pattern,
                Context ctx,
                Collector<Tuple2<String, Pattern>> out) throws Exception {

            BroadcastState<Void, Pattern> bcState = ctx.getBroadcastState(
                    new MapStateDescriptor<>("patterns", Types.VOID, Types.POJO(Pattern.class)));

            // 将广播状态更新为当前的pattern
            bcState.put(null, pattern);
        }

        @Override
        public void processElement(Action action, ReadOnlyContext ctx,
                                   Collector<Tuple2<String, Pattern>> out) throws Exception {
            Pattern pattern = ctx.getBroadcastState(
                    new MapStateDescriptor<>("patterns", Types.VOID, Types.POJO(Pattern.class))).get(null);

            String prevAction = prevActionState.value();
            if (pattern != null && prevAction != null) {
                // 如果前后两次行为都符合模式定义，输出一组匹配
                if (pattern.action1.equals(prevAction) && pattern.action2.equals(action.action)) {
                    out.collect(new Tuple2<>(ctx.getCurrentKey(), pattern));
                }
            }
            // 更新状态
            prevActionState.update(action.action);
        }
    }

    // 定义用户行为事件POJO类
    public static class Action {
        public String userId;
        public String action;

        public Action() {
        }



        public Action(String userId, String action) {
            this.userId = userId;
            this.action = action;
        }

        @Override
        public String toString() {
            return "Action{" +
                    "userId=" + userId +
                    ", action='" + action + '\'' +
                    '}';
        }
    }

    // 定义行为模式POJO类，包含先后发生的两个行为
    public static class Pattern {
        public String action1;
        public String action2;

        public Pattern() {
        }

        public Pattern(String action1, String action2) {
            this.action1 = action1;
            this.action2 = action2;
        }

        @Override
        public String toString() {
            return "Pattern{" +
                    "action1='" + action1 + '\'' +
                    ", action2='" + action2 + '\'' +
                    '}';
        }
    }
}
```

## 状态持久化和状态后端

### 检查点

当前状态的一个快照，进行持久化的保存



在 Flink 的状态管理机制中，很重要的一个功能就是对状态进行持久化（persistence）保存，这样就可以在发生故障后进行重启恢复。Flink 对状态进行持久化的方式，就是将当前所有分布式状态进行“快照”保存，写入一个“检查点”（checkpoint）或者保存点（savepoint）保存到外部存储系统中。具体的存储介质，一般是分布式文件系统（distributed file system）



打开检查点

```java
env.enableCheckpointing(10000L);
```



检查点的保存离不开 JobManager 和 TaskManager，以及外部存储系统的协调。在应用进行检查点保存时，首先会由 JobManager 向所有 TaskManager 发出触发检查点的命令； TaskManger 收到之后，将当前任务的所有状态进行快照保存，持久化到远程的存储介质中； 完成之后向 JobManager 返回确认信息。这个过程是分布式的，当 JobManger 收到所有TaskManager 的返回信息后，就会确认当前检查点成功保存，如下图所示。而这一切工作的协调，就需要一个“专职人员”来完成。

![1660547130950](https://gitee.com/it-wont-work/typora-cloud-map-library/raw/master/img/1660547130950.png)

   

在 Flink 中，状态的存储、访问以及维护，都是由一个可插拔的组件决定的，这个组件就叫作状态后端（state backend）。状态后端主要负责两件事：**一是本地的状态管理，二是将检查点（checkpoint）写入远程的持久化存储**



状态后端是一个“开箱即用”的组件，可以在不改变应用程序逻辑的情况下独立配置。Flink 中提供了两类不同的状态后端，一种是“哈希表状态后端”（HashMapStateBackend），另一种是“内嵌 RocksDB  状态后端”（EmbeddedRocksDBStateBackend）。如果没有特别配置，系统默认的状态后端是HashMapStateBackend。

（1）哈希表状态后端（HashMapStateBackend）

这种方式就是我们之前所说的，把状态存放在内存里。具体实现上，哈希表状态后端在内部会直接把状态当作对象（objects），保存在 Taskmanager 的 JVM 堆（heap）上。普通的状态，以及窗口中收集的数据和触发器（triggers），都会以键值对（key-value）的形式存储起来，所以底层是一个哈希表（HashMap），这种状态后端也因此得名。

对于检查点的保存，一般是放在持久化的分布式文件系统（file system）中，也可以通过配置“检查点存储”（CheckpointStorage）来另外指定。

HashMapStateBackend 是将本地状态全部放入内存的，这样可以获得最快的读写速度，使计算性能达到最佳；代价则是内存的占用。它适用于具有大状态、长窗口、大键值状态的作业， 对所有高可用性设置也是有效的。

（2）内嵌RocksDB 状态后端（EmbeddedRocksDBStateBackend）

RocksDB 是一种内嵌的 key-value 存储介质，可以把数据持久化到本地硬盘。配置EmbeddedRocksDBStateBackend 后，会将处理中的数据全部放入 RocksDB 数据库中，RocksDB 默认存储在TaskManager 的本地数据目录里。

与 HashMapStateBackend 直接在堆内存中存储对象不同，这种方式下状态主要是放在RocksDB 中的。数据被存储为序列化的字节数组（Byte Arrays），读写操作需要序列化/反序列化，因此状态的访问性能要差一些。另外，因为做了序列化，key 的比较也会按照字节进行， 而不是直接调用.hashCode()和.equals()方法。



### 两种状态后端区别

HashMapStateBackend 本地状态存放在内存，读写快，存储状态有限

EmbeddedRocksDBStateBackend本地状态存放在本地硬盘，读写速度慢，能存储海量状态

检查点的保存，都是放在持久化的分布式文件系统（file system）中，一般为hdfs



flink-conf.yaml

```shell
# 默认状态后端
state.backend: hashmap 

state.backend: rocksdb

# 存放检查点的文件路径
state.checkpoints.dir: hdfs://namenode:40010/flink/checkpoints
```



代码设置

```java
env.setStateBackend(new HashMapStateBackend());
```

代码中rockdb需要加入依赖



压测方式：kafka中积压数据，再开启fink消费，出现反压，就是处理瓶颈、也可以模拟数据



# 容错机制

## 检查点的保存

* 周期性的保存，节约资源
* 保存的时间点：当所有任务都恰好处理完一个相同的输入数据的时候，将它们的状态保存下来。类似于事务，一个数据要么就是被所有任务完整地处理完，状态得到了保存；要么就是没处理完，状态全部没保存



checkpoint机制

第1步：由Job Manager初始化Checkpoint，在数据源之后放一个barrier，以此为隔断

第2步：将所有barrier下游的数据都计算完，并将CheckPoint的source、数据源的offset和最终计算的Result上报至State，存好

一旦任务发生故障，重启任务，到State中读取所有任务元数据，重来一遍就好了。

### 检查点分界线

![1660552245657](https://gitee.com/it-wont-work/typora-cloud-map-library/raw/master/img/1660552245657.png)

watermark 指示的是“之前的数据全部到齐了”，而 barrier 指示的是“之前所有数据的状态更改保存入当前检查点”：它们都是一个“截止时间”的标志。



等到并行的barrier都到达时，进行状态的保存



## 检查点配置

![1660556495828](https://gitee.com/it-wont-work/typora-cloud-map-library/raw/master/img/1660556495828.png)



## savepoint和checkpoint区别

savepoint手动，checkpoint设置完之后自动



保存点用法

```shell
# 创建保存点
bin/flink savepoint :jobId [:targetDirectory]
bin/flink stop --savepointPath [:targetDirectory] :jobId
#从保存点重启应用
bin/flink run -s :savepointPath [:runArgs]
```



配置文件修改默认路径

```yml
state.savepoints.dir: hdfs:///flink/savepoints
```

代码修改

```java
env.setDefaultSavepointDir("hdfs:///flink/savepoints")
```



## 状态一致性

级别

* 最多一次（AT-MOST-ONCE）
* 至少一次（AT-LEAST-ONCE）
* 精确一次（EXACTLY-ONCE）



* Flink 内部
  * Flink 内部可以通过检查点机制保证状态和处理结果的 exactly-once 语义
* source ——可重设数据的读取位置
* sink端 ——从故障恢复时，数据不会重复写入外部系统
  * 幂等写入
  * 事务写入



幂等写入：

幂等操作，一个操作可以重复执行很多次，但只导致一次结果更改



事务写入

事务：应用程序中一系列严密的操作，所有操作必须成功完成，否则每个操作所作的所有更改都会被撤销

具有原子性：一个事务的一系列操作要么全部成功，要么一个都不做

思想：构建的事务对应checkpoint，等到checkpoint真正完成，才把所有对应结果写入sink

实现方式

* 预写日志（Write-Ahead-Log）WAL
  * 把结果数据先当成状态保存，然后在收到checkpoint完成的通知时，一次性写入sink系统
  * 简单易于实现，由于数据提交在状态后端做了缓存，无论什么sink系统，都能用这种方式一批搞定
  * DataStream API 提供了模板类：GenericWriteAheadSink，来实现这种事务性sink
* 两阶段提交（Two-Phase-Commit,2PC）
  * 对于每个ck，sink任务会启动一个事务。并将接下来所有接收的数据添加到事务里
  * 然后将数据写入外部sink，但不提交他们，只是“预提交”
  * 当他收到ck完成的通知时，它才正式提交事务，实现结果的真正写入
  * 这种方式真正实现了exactly-once，他需要一个提供事务支持的外部sink系统。flink提供了twoPhaseCommitSinkFunction接口

区别：WAL是批量处理，

2PC是流式写入，而且能够保证exactly-once，需要外部支持事务



WAL能满足一些场景，但是只有当数据写入sink后才能确认ck写入成功，需要sink端向taskmanager发送确认信息，此时如果这个二次确认失败，就会导致重复写入，只能是至少一次了



对于幂等写入，遇到故障进行恢复时，有可能会出现短暂的不一致。因为保存点完成之后到发生故障之间的数据，其实已经写入了一遍，回滚的时候并不能消除它们。如果有一个外部应用读取写入的数据，可能会看到奇怪的现象：短时间内，结果会突然“跳回”到之前的某个值，然后“重播”一段之前的数据。不过当数据的重放逐渐超过发生故障的点的时候，最终的结果还是一致的



| sink | source不可重置 |           source 可重置            |
| :--: | :------------: | :--------------------------------: |
| 任意 |  At-most-once  |           At-least-once            |
| 幂等 |  At-most-once  | Exactly-once(故障恢复时短暂不一致) |
| WAL  |  At-most-once  |           At-least-once            |
| 2PC  |  At-most-once  |            Exactly-once            |



# Table API 和SQL



对于 Flink 这样的流处理框架来说，数据流和表在结构上还是有所区别的。所以使用 Table API 和 SQL 需要一个特别的运行时环境，这就是所谓的“表环境”（TableEnvironment）。它主要负责：

（1） 注册Catalog 和表；

（2） 执行 SQL 查询；

（3） 注册用户自定义函数（UDF）；

（4） DataStream 和表之间的转换。

这里的 Catalog 就是“目录”，与标准 SQL 中的概念是一致的，主要用来管理所有数据库

（database）和表（table）的元数据（metadata）。通过 Catalog  可以方便地对数据库和表进行查询的管理，所以可以认为我们所定义的表都会“挂靠”在某个目录下，这样就可以快速检索。在表环境中可以由用户自定义Catalog，并在其中注册表和自定义函数（UDF）。默认的 Catalog就叫作 default_catalog



## 创建表环境

```java
//基于DS环境        
StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
```



```java
        // 定义环境配置来创建表
        // 基于blink版本planner进行流处理
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();

        TableEnvironment tableEnv = TableEnvironment.create(settings);
```





## 创建表

![1660598999085](https://gitee.com/it-wont-work/typora-cloud-map-library/raw/master/img/1660598999085.png)

虚拟表

```java
tableEnv.createTemporaryView("NewTable", newTable);
```

注册表转化为Table对象

```java
Table clickTable = tableEnv.from("clickTable");
```



## 表的查询

Flink 基于 Apache Calcite 来提供对SQL 的支持，Calcite 是一个为不同的计算平台提供标准 SQL 查询的底层工具，很多大数据框架比如 Apache Hive、Apache Kylin 中的SQL 支持都是通过集成 Calcite 来实现的

Table API查询

```java
Table resultTable = clickTable.where($("user_name").isEqual("Bob"))
                .select($("user_name"), $("url"));
```

执行SQL查询

```java
Table resultTable2 = tableEnv.sqlQuery("select url, user_name from resultTable");
```



## 输出表

```java
        String createPrintOutDDL = "CREATE TABLE printOutTable (" +
                " user_name STRING, " +
                " cnt BIGINT " +
                ") WITH (" +
                " 'connector' = 'print' " +
                ")";
```



## 表和流的转换

```java
//表转换成流，适用于追加流
tableEnv.toDataStream(resultTable1).print("result1);
//有更新的操作流，更通用，更新日志流
tableEnv.toChangelogStream(urlCountTable).print("count");
```



```java
//流转换成表
Table eventTable = tableEnv.fromDataStream(eventStream);
Table eventTable2 = tableEnv.fromDataStream(eventStream, $("timestamp"),
$("url")); 
Table eventTable2 = tableEnv.fromDataStream(eventStream, $("timestamp").as("ts"),
$("url"));
 //将流转换为动态表
tableEnv.createTemporaryView("t1", waterSensorDS1);
```



```java
//Tupple
Table table = tableEnv.fromDataStream(stream, $("f1"), $("f0"));
Table table = tableEnv.fromDataStream(stream, $("f1").as("myInt"),
$("f0").as("myLong"));
//POJO
Table table = tableEnv.fromDataStream(stream, $("user").as("myUser"),
$("url").as("myUrl"));

//ROW
DataStream<Row> dataStream = env.fromElements(to
Row.ofKind(RowKind.INSERT, "Alice", 12),
Row.ofKind(RowKind.INSERT, "Bob", 5),
Row.ofKind(RowKind.UPDATE_BEFORE, "Alice", 12),
Row.ofKind(RowKind.UPDATE_AFTER, "Alice", 100));

// 将更新日志流转换为表
Table table = tableEnv.fromChangelogStream(dataStream);
```



## 动态表和持续查询

**动态表**

当流中有新数据到来，初始的表中会插入一行；而基于这个表定义的 SQL 查询，就应该在之前的基础上更新结果。这样得到的表就会不断地动态变化，被称为“动态表”（Dynamic Tables）。

动态表是Flink 在Table API 和SQL 中的核心概念，它为流数据处理提供了表和SQL 支持。我们所熟悉的表一般用来做批处理，面向的是固定的数据集，可以认为是“静态表”；而动态表则完全不同，它里面的数据会随时间变化。

其实动态表的概念，我们在传统的关系型数据库中已经有所接触。数据库中的表，其实是一系列 INSERT、UPDATE 和 DELETE 语句执行的结果；在关系型数据库中，我们一般把它称为更新日志流（changelog stream）。如果我们保存了表在某一时刻的快照（snapshot），那么接下来只要读取更新日志流，就可以得到表之后的变化过程和最终结果了。在很多高级关系型数据库（比如 Oracle、DB2）中都有“物化视图”（Materialized Views）的概念，可以用来缓存 SQL 查询的结果；它的更新其实就是不停地处理更新日志流的过程。

Flink 中的动态表，就借鉴了物化视图的思想。



**持续查询**

动态表可以像静态的批处理表一样进行查询操作。由于数据在不断变化，因此基于它定义的 SQL 查询也不可能执行一次就得到最终结果。这样一来，我们对动态表的查询也就永远不会停止，一直在随着新数据的到来而继续执行。这样的查询就被称作“持续查询”（Continuous Query）。对动态表定义的查询操作，都是持续查询；而持续查询的结果也会是一个动态表。

由于每次数据到来都会触发查询操作，因此可以认为一次查询面对的数据集，就是当前输入动态表中收到的所有数据。这相当于是对输入动态表做了一个“快照”（snapshot），当作有限数据集进行批处理；流式数据的到来会触发连续不断的快照查询，像动画一样连贯起来，就构成了“持续查询”

![1660605673359](C:\Users\Lin\AppData\Roaming\Typora\typora-user-images\1660605673359.png)

持续查询的**步骤**如下：

（1） 流（stream）被转换为动态表（dynamic table）；

（2） 对动态表进行持续查询（continuous query），存储状态，生成新的动态表；

（3） 生成的动态表被转换成流。



## 时间属性和窗口

DDL 创建时间属性

![1660648672560](https://gitee.com/it-wont-work/typora-cloud-map-library/raw/master/img/1660648672560.png)

```java
// 方法一:
// 流中数据类型为二元组 Tuple2，包含两个字段；需要自定义提取时间戳并生成水位线
DataStream<Tuple2<String, String>> stream = inputStream.assignTimestampsAndWatermarks(...);
// 声明一个额外的逻辑字段作为事件时间属性
Table table = tEnv.fromDataStream(stream, $("user"), $("url"),
$("ts").rowtime());
// 方法二:
// 流中数据类型为三元组 Tuple3，最后一个字段就是事件时间戳
DataStream<Tuple3<String, String, Long>> stream = inputStream.assignTimestampsAndWatermarks(...);
// 不再声明额外字段，直接用最后一个字段作为事件时间属性
Table table = tEnv.fromDataStream(stream, $("user"), $("url"),
$("ts").rowtime());

//处理时间
Table table = tEnv.fromDataStream(stream, $("user"), $("url"),
$("ts").proctime());
```



**分组窗口**

Table API 和 SQL 提供了一组“分组窗口”（Group Window）函数，常用的时间窗口如滚动窗口、滑动窗口、会话窗口都有对应的实现；具体在 SQL 中就是调用 TUMBLE()、HOP()、SESSION()，传入时间属性字段、窗口大小等参数就可以了。以滚动窗口为例

```java
Table result = tableEnv.sqlQuery(
"SELECT " +
"user, " +
"TUMBLE_END(ts, INTERVAL '1' HOUR) as endT, " +
"COUNT(url) AS cnt " + "FROM EventTable " +
"GROUP BY " +	// 使用窗口和用户名进行分组
"user, " +
"TUMBLE(ts, INTERVAL '1' HOUR)" // 定义 1 小时滚动窗口
);
```



**窗口表值函数**

从 1.13 版本开始，Flink 开始使用窗口表值函数（Windowing table-valued functions， Windowing  TVFs）来定义窗口

```java
//滚动
TUMBLE(TABLE EventTable, DESCRIPTOR(ts), INTERVAL '1' HOUR)
//滑动
HOP(TABLE EventTable, DESCRIPTOR(ts), INTERVAL '5' MINUTES, INTERVAL '1' HOURS));
//累积
CUMULATE(TABLE EventTable, DESCRIPTOR(ts), INTERVAL '1' HOURS, INTERVAL '1' DAYS))
```



示例

```java
package com.atguigu.chapter11;

import com.atguigu.chapter05.ClickSource;
import com.atguigu.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial
 * <p>
 * Created by  wushengran
 */

public class TimeAndWindowTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1. 在创建表的DDL中直接定义时间属性
        String createDDL = "CREATE TABLE clickTable (" +
                " user_name STRING, " +
                " url STRING, " +
                " ts BIGINT, " +
                " et AS TO_TIMESTAMP( FROM_UNIXTIME(ts / 1000) ), " +
                " WATERMARK FOR et AS et - INTERVAL '1' SECOND " +
                ") WITH (" +
                " 'connector' = 'filesystem', " +
                " 'path' = 'input/clicks.csv', " +
                " 'format' =  'csv' " +
                ")";

        tableEnv.executeSql(createDDL);

        // 2. 在流转换成Table时定义时间属性
        SingleOutputStreamOperator<Event> clickStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));

        Table clickTable = tableEnv.fromDataStream(clickStream, $("user"), $("url"), $("timestamp").as("ts"),
                $("et").rowtime());

//        clickTable.printSchema();

        // 聚合查询转换

        // 1. 分组聚合
        Table aggTable = tableEnv.sqlQuery("SELECT user_name, COUNT(1) FROM clickTable GROUP BY user_name");

        // 2. 分组窗口聚合
        Table groupWindowResultTable = tableEnv.sqlQuery("SELECT " +
                "user_name, " +
                "COUNT(1) AS cnt, " +
                "TUMBLE_END(et, INTERVAL '10' SECOND) as endT " +
                "FROM clickTable " +
                "GROUP BY " +                     // 使用窗口和用户名进行分组
                "  user_name, " +
                "  TUMBLE(et, INTERVAL '10' SECOND)" // 定义1小时滚动窗口
        );

        // 3. 窗口聚合
        // 3.1 滚动窗口
        Table tumbleWindowResultTable = tableEnv.sqlQuery("SELECT user_name, COUNT(url) AS cnt, " +
                " window_end AS endT " +
                "FROM TABLE( " +
                "  TUMBLE( TABLE clickTable, DESCRIPTOR(et), INTERVAL '10' SECOND)" +
                ") " +
                "GROUP BY user_name, window_start, window_end "
        );

        // 3.2 滑动窗口
        Table hopWindowResultTable = tableEnv.sqlQuery("SELECT user_name, COUNT(url) AS cnt, " +
                " window_end AS endT " +
                "FROM TABLE( " +
                "  HOP( TABLE clickTable, DESCRIPTOR(et), INTERVAL '5' SECOND, INTERVAL '10' SECOND)" +
                ") " +
                "GROUP BY user_name, window_start, window_end "
        );

        // 3.3 累积窗口
        Table cumulateWindowResultTable = tableEnv.sqlQuery("SELECT user_name, COUNT(url) AS cnt, " +
                " window_end AS endT " +
                "FROM TABLE( " +
                "  CUMULATE( TABLE clickTable, DESCRIPTOR(et), INTERVAL '5' SECOND, INTERVAL '10' SECOND)" +
                ") " +
                "GROUP BY user_name, window_start, window_end "
        );

        // 4. 开窗聚合
        Table overWindowResultTable = tableEnv.sqlQuery("SELECT user_name, " +
                " avg(ts) OVER (" +
                "   PARTITION BY user_name " +
                "   ORDER BY et " +
                "   ROWS BETWEEN 3 PRECEDING AND CURRENT ROW" +
                ") AS avg_ts " +
                "FROM clickTable");

        // 结果表转换成流打印输出
//        tableEnv.toChangelogStream(aggTable).print("agg: ");
//        tableEnv.toDataStream(groupWindowResultTable).print("group window: ");
//        tableEnv.toDataStream(tumbleWindowResultTable).print("tumble window: ");
//        tableEnv.toDataStream(hopWindowResultTable).print("hop window: ");
//        tableEnv.toDataStream(cumulateWindowResultTable).print("cumulate window: ");
        tableEnv.toDataStream(overWindowResultTable).print("over window: ");

        env.execute();
    }
}

```



# 实时数仓

## 分层

实时数仓分层:
	计算框架:Flink;存储框架:消息队列(可以实时读取&可以实时写入)
	ODS:Kafka
		使用场景:每过来一条数据,读取到并加工处理
	DIM:HBase
		使用场景:事实表会根据主键获取一行维表数据(1.永久存储、2.根据主键查询)
		HBase:海量数据永久存储,根据主键快速查询（ROWKEY为主键快速原因：ROWKEY会排序，还会给ROWKEY建立索引），Hbase可行可列，把列族设计为1个 就是行存储         √
		Redis:用户表数据量大,内存数据库                 ×
		ClickHouse:并发不行,列存（根据主键查询，行存最好）                       ×
		ES:默认给所有字段创建索引                       ×
		Hive(HDFS):效率低下                            ×
		Mysql本身:压力太大,实在要用就使用从库（不进行写操作的读库）            √
	DWD:Kafka
		使用场景:每过来一条数据,读取到并分组累加处理
	DWS:ClickHouse
		使用场景:每过来一条数据,读取到并重新分组、累加处理
	ADS:不落盘,实质上是接口模块中查询ClickHouse的SQL语句
		使用场景:读取最终结果数据展示



# 分层需求

## dim层 

广播从flinkcdc读取到的配置表，将数据写入dim层（flatmap、FlinkDCD、德鲁伊连接池、FlinkKafkaConsumer，广播流）

![1660826207884](https://gitee.com/it-wont-work/typora-cloud-map-library/raw/master/img/1660826207884.png)

## dwd层 

### 日志表分流写入事实表（流量域）

![1660821914261](https://gitee.com/it-wont-work/typora-cloud-map-library/raw/master/img/1660821914261.png)

分流对日志数据进行拆分，生成五张事务事实表写入 Kafka

使用侧输出流,localdatetime工具类

actions和display是json数组，需要遍历完进行输出

### 独立访客需求事实表（流量域）

日活(过滤页面数据中的独立访客访问记录)

![1660878420012](https://gitee.com/it-wont-work/typora-cloud-map-library/raw/master/img/1660878420012.png)

代码思路

1. 获取执行环境

2. 读取Kafka 页面日志主题创建流

3. 过滤掉上一跳页面不为null的数据并将每行数据转换为JSON对象

4. 按照Mid分组

5. 使用状态编程实现按照Mid的去重（使用了**TTL**）

   ![1660879420341](https://gitee.com/it-wont-work/typora-cloud-map-library/raw/master/img/1660879420341.png)

6. 将数据写到Kafka



```java
数据流：web/app -> Nginx -> 日志服务器(.log) -> Flume -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> Kafka(DWD)
程  序：     Mock(lg.sh) -> Flume(f1) -> Kafka(ZK) -> BaseLogApp -> Kafka(ZK) -> DwdTrafficUniqueVisitorDetail -> Kafka(ZK)
```



### 跳出事务事实表（流量域）

![1660883297598](https://gitee.com/it-wont-work/typora-cloud-map-library/raw/master/img/1660883297598.png)



代码思路

1. 获取执行环境

2. 读取Kafka 页面日志主题数据创建流

3. 将每行数据转换为JSON对象

4. 提取事件时间&按照Mid分组

5. 定义**CEP**的模式序列

   ![1660885400069](https://gitee.com/it-wont-work/typora-cloud-map-library/raw/master/img/1660885400069.png)

6. 将模式序列作用到流上

   ![1660885599358](https://gitee.com/it-wont-work/typora-cloud-map-library/raw/master/img/1660885599358.png)

7. 提取事件(匹配上的事件以及超时事件)

   ![1660886285239](https://gitee.com/it-wont-work/typora-cloud-map-library/raw/master/img/1660886285239.png)

8. 合并两个种事件

   ![1660886308805](https://gitee.com/it-wont-work/typora-cloud-map-library/raw/master/img/1660886308805.png)

9. 将数据写出到Kafka

   ![1660886344342](https://gitee.com/it-wont-work/typora-cloud-map-library/raw/master/img/1660886344342.png)

10. 启动任务



```java
数据流：web/app -> Nginx -> 日志服务器(.log) -> Flume -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> Kafka(DWD)
程  序：     Mock(lg.sh) -> Flume(f1) -> Kafka(ZK) -> BaseLogApp -> Kafka(ZK) -> DwdTrafficUserJumpDetail -> Kafka(ZK)
```



### 加购事务事实表（交易域）

需求：提取加购操作生成加购表，并将字典表中的相关维度退化到加购表中，写出到Kafka对应的主题

![1660908860227](https://gitee.com/it-wont-work/typora-cloud-map-library/raw/master/img/1660908860227.png)

代码思路

1. 获取执行环境

2. 使用DDL方式读取 topic_db 主题的数据创建表

   PROCTIME() 函数获取系统时间

   data、old类型为**Map**

   ![1660923617283](https://gitee.com/it-wont-work/typora-cloud-map-library/raw/master/img/1660923617283.png)

3. 过滤出加购数据

4. 读取MySQL的 base_dic 表作为LookUp表

5. 关联两张表

6. 使用DDL方式创建加购事实表

7. 将数据写出

8. 启动任务



### 订单预处理表（交易域）

需求：经过分析，订单明细表和取消订单明细表的数据来源、表结构都相同，差别只在业务过程和过滤条件，为了减少重复计算，将两张表公共的关联过程提取出来，形成订单预处理表。

关联订单明细表、订单表、订单明细活动关联表、订单明细优惠券关联表四张事实业务表和字典表（维度业务表）形成订单预处理表，写入 Kafka 对应主题。

本节形成的预处理表中要保留订单表的 type 和 old 字段，用于过滤订单明细数据和取消订单明细数据

![1660935306058](https://gitee.com/it-wont-work/typora-cloud-map-library/raw/master/img/1660935306058.png)



代码思路

1. 获取执行环境

2. 创建 topic_db 表

3. 过滤出订单明细数据

4. 过滤出订单数据

5. 过滤出订单明细活动关联数据

6. 过滤出订单明细购物券关联数据

7. 创建 base_dic LookUp表

   关联渠道（智能推荐、用户推荐）

8. 关联5张表

   ```java
   //使用left join需要用撤回流打印输出
   //tableEnv.toRetractStream(resultTable, Row.class).print(">>>>");
   ```

9. 创建 upsert-kafka 表

   因为用到left join 有撤回的操作，所以需要用到kafka upsert 连接器 他和kafka连接器的区别在于，需要主键

   ![1660974057678](https://gitee.com/it-wont-work/typora-cloud-map-library/raw/master/img/1660974057678.png)

   ![21](https://gitee.com/it-wont-work/typora-cloud-map-library/raw/master/img/21.png)

10. 将数据写出

11. 启动任务



### 下单事务事实表(交易域)

从 Kafka 读取订单预处理表数据，筛选下单明细数据，写入 Kafka 对应主题

![1661157987191](https://gitee.com/it-wont-work/typora-cloud-map-library/raw/master/img/1661157987191.png)



## dws

### 流量域来源关键词粒度页面浏览各窗口汇总表

![1661106837036](https://gitee.com/it-wont-work/typora-cloud-map-library/raw/master/img/1661106837036.png)



代码思路

1. 获取执行环境

2. 使用DDL方式读取Kafka page_log 主题的数据创建表并且提取时间戳生成Watermark

   ![1661108724248](https://gitee.com/it-wont-work/typora-cloud-map-library/raw/master/img/1661108724248.png)

   ```java
   //FROM_UNIXTIME  时间戳转日期
   //TO_TIMESTAMP   日期转timestamp
   ```

3. 过滤出搜索数据

4. 注册UDTF & 切词

   ![1661109475558](https://gitee.com/it-wont-work/typora-cloud-map-library/raw/master/img/1661109475558.png)

5. 分组、开窗、聚合

   ![1661111779650](https://gitee.com/it-wont-work/typora-cloud-map-library/raw/master/img/1661111779650.png)

6. 将动态表转换为流

   ![1661111879309](https://gitee.com/it-wont-work/typora-cloud-map-library/raw/master/img/1661111879309.png)

7. 将数据写出到ClickHouse

8. 启动任务



数据格式

![1661108531325](C:\Users\Lin\AppData\Roaming\Typora\typora-user-images\1661108531325.png)





clickhouse引擎选择

 ReplacingMergeTree   在任务挂掉重新消费时可以去重，可以保证数据一致性，它具有幂等性

不会对数据进行聚合，可以进行分时统计

缺点 数据量大



clickhouse中一个分区是一个目录



建表语句

```sql
drop table if exists dws_traffic_source_keyword_page_view_window;
create table if not exists dws_traffic_source_keyword_page_view_window
(
    stt           DateTime,
    edt           DateTime,
    source        String,
    keyword       String,
    keyword_count UInt64,
    ts            UInt64  --版本取系统时间 数据重复时取时间大的
) engine = ReplacingMergeTree(ts)
      partition by toYYYYMMDD(stt) --按天分区
      order by (stt, edt, source, keyword); --窗口时间和关键词去重
```



### 流量域版本-渠道-地区-访客类别粒度页面浏览各窗口汇总表



#### 图解



![1661151474856](https://gitee.com/it-wont-work/typora-cloud-map-library/raw/master/img/1661151474856.png)



#### 代码思路

1. 获取执行环境
2. 读取三个主题的数据创建流
3. 统一数据格式
4. 将三个流进行Union
5. 提取事件时间生成WaterMark
6. 分组开窗聚合
7. 将数据写出到ClickHouse
8. 启动任务



![1662106474982](https://gitee.com/it-wont-work/typora-cloud-map-library/raw/master/img/1662106474982.png)



### 交易域用户-SPU粒度下单各窗口汇总表

![1661167096745](https://gitee.com/it-wont-work/typora-cloud-map-library/raw/master/img/1661167096745.png)



在开窗聚合后再关联其他维度表字段是因为聚合后数据量变小，可以减少对数据库的访问



1. 获取执行环境
2. 读取Kafka DWD层下单主题数据创建流
3. 将每行数据转换为JSON对象
4. 按照 order_detail_id 分组
5. 针对 order_detail_id 进行去重(保留第一条数据即可)
6. 将数据转换为JavaBean对象
7. 关联sku_info维表 补充 spu_id,tm_id,category3_id
8. 提取事件时间生成Watermark
9. 分组、开窗、聚合
10. 关联spu,tm,category维表补充相应的信息
11. 将数据写出到ClickHouse
12. 启动



优化：异步IO同时发送请求，减少等待时间



两种方式 ：1 使用数据库异步客户端 2 使用数据库同步客户端+多线程

采用第二种方式 因为工具类写好了 且更通用（如果数据库不提供异步客户端）



反射 JDBC util



![1662108503463](https://gitee.com/it-wont-work/typora-cloud-map-library/raw/master/img/1662108503463.png)





# 调优

## 资源配置调优 

- jobmanager内存：2G
- taskmanager slot 数： container中cpu和slot的比例： 1：1    2个CPU 2个核
- taskmanager：1个CPU4G内存，所以8G



```shell
bin/flink run \
-t yarn-per-job \
-d \
-p 5 \ 指定并行度
-Dyarn.application.queue=test \ 指定 yarn 队列
-Djobmanager.memory.process.size=2048mb \ JM2~4G 足够
-Dtaskmanager.memory.process.size=4096mb \ 单个 TM2~8G 足够
-Dtaskmanager.numberOfTaskSlots=2 \ 与容器核数 1core：1slot 或 2core：1slot
-c com.atguigu.flink.tuning.UvDemo \
/opt/module/flink-1.13.1/myjar/flink-tuning-1.0-SNAPSHOT.jar
```



source的数据源端是kafka,Source的并行度设置为Kafka对应的topic分区数



RocksDB  磁盘+内存的模式 能开启增量检查点



## 反压

![1663088811476](https://gitee.com/it-wont-work/typora-cloud-map-library/raw/master/img/1663088811476.png)



定位：下游为绿，自己红的算子



三种原因和解决方案

资源不足：提高并行度、内存、加机器

数据倾斜：只有个别Backpressure status为红,

和第三方数据库交互：使用旁路缓存和异步IO解决





## 数据倾斜

现象：相同 Task 的多个 Subtask 中， 个别 Subtask 接收到的数据量明显大于其他Subtask 接收到的数据量，通过 Flink Web UI 可以精确地看到每个 Subtask 处理了多少数据，即可判断出 Flink 任务是否存在数据倾斜。通常，数据倾斜也会引起反压

keyby之前数据倾斜：rebalance 重分区全局轮询，尽量在上游处理

keyby之后数据倾斜

- keyby后直接聚合（来一条处理一条）

  - 两阶段聚合会导致数据重复计算，不准确，不采用
  - 预聚合：定时器+状态(普通的算子状态)，采用，开窗聚合不行：会使用keyby

- keyby后开窗聚合(一个窗口输出一条)

  - 加随机数实现两阶段聚合，采用，
    - 第一阶段key拼接随机数前缀，进行keyby开窗
    - 第二阶段聚合：去掉随机数前缀，加上windowend(窗口结束时间)作为key

  ![1663087797387](https://gitee.com/it-wont-work/typora-cloud-map-library/raw/master/img/1663087797387.png)

  - 预聚合：会丢失原始的数据时间，时间语义就没了

# 杂记

## 快速原因

流式处理，来一条处理一条

状态保存在内存中，并通过分布式来提高吞吐量



## 富含数和处理函数区别

富含数：包含生命周期，有open、close方法，以及上下文可以进行状态编程

处理函数：包含定时器、侧输出流



## 日期类

SimpleDateFormat 存在线程安全问题

多个线程可以操作同一个资源，写操作

```java
//日期转时间戳
LocalDateTime.parse(dtStr, dtfFull);
//时间戳转日期
dtfFull.format(localDateTime);
```



## kafka消费者组不一样

同一个组只能消费一次数据，所以不同应用必须消费者组不一致



## 状态

状态：可以理解为历史数据



## WaterMark

WaterMark可以结合开窗处理乱序数据，表示小于WaterMark数据已经到齐



## 几种Join

滚动窗口join:会丢数据

滑动窗口join:会有重复数据

会话窗口join:会话窗口可能源源不断有数据，导致窗口过大，实效性差



## interval join

The interval join currently only supports event time.



## table join过期时间

时间语义为process time

![1660903376407](https://gitee.com/it-wont-work/typora-cloud-map-library/raw/master/img/1660903376407.png)



状态的保存 

![1660905394161](https://gitee.com/it-wont-work/typora-cloud-map-library/raw/master/img/1660905394161.png)



## 表和流的转换

```java
 //将流转换为动态表
        tableEnv.createTemporaryView("t1", waterSensorDS1);
```



## sublime tips

选中指定内容行

1. ALT+F3选中内容
2. Shitf+Fn+Home
3. ←
4. Shitf+Fn+End
5. Ctrl c复制



行尾加逗号

1. `Ctrl+A`
2. `Ctrl+Shift+L`
3. 使用左右方向键（←或者→）



选中多行

Ctrl+Alt+↓



## Lookup Cache

JDBC 连接器可以作为时态表关联中的查询数据源（又称维表）。目前，仅支持同步查询模式。

默认情况下，查询缓存（Lookup Cache）未被启用，需要设置 lookup.cache.max-rows 和 lookup.cache.ttl 参数来启用此功能。

Lookup 缓存是用来提升有 JDBC 连接器参与的时态关联性能的。默认情况下，缓存未启用，所有的请求会被发送到外部数据库。当缓存启用时，每个进程（即 TaskManager）维护一份缓存。收到请求时，Flink 会先查询缓存，如果缓存未命中才会向外部数据库发送请求，并用查询结果更新缓存。如果缓存中的记录条数达到了 lookup.cache.max-rows 规定的最大行数时将清除存活时间最久的记录。如果缓存中的记录存活时间超过了 lookup.cache.ttl 规定的最大存活时间，同样会被清除。

缓存中的记录未必是最新的，可以将 lookup.cache.ttl 设置为一个更小的值来获得时效性更好的数据，但这样做会增加发送到数据库的请求数量。所以需要在吞吐量和正确性之间寻求平衡。



## Lookup Join 

通常在 Flink SQL 表和外部系统查询结果关联时使用。这种关联要求一张表（主表）有处理时间字段，而另一张表（维表）由 Lookup 连接器生成。

Lookup Join 做的是维度关联，而维度数据是有时效性的，那么我们就需要一个时间字段来对数据的版本进行标识。因此，Flink 要求我们提供处理时间用作版本字段。

此处选择调用 PROCTIME() 函数获取系统时间，将其作为处理时间字段。该函数调用示例如下



## 增量窗口函数和全量窗口函数区别

增量聚合函数:来一条计算一条、效率高、存储的数据量小
全量聚合函数:攒到一个集合中,最后统一计算、可以求前百分比、可以获取窗口信息

经常使用AggregateFunction（ReduceFunction） + ProcessWindowFunction（windowFunction） 配合使用，既能流式处理，又可以得到上下文的信息



反射 JDBC util