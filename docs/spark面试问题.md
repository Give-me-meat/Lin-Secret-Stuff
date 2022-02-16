# spark面试问题

# spark job的提交流程

1. 通过spark submit提交任务信息到（resourceManager)
2. client向resourceManager发起请求，申请启动ApplicationMaster
3. RM在一台NodeManager上启动ApplicationMaster,在ApplicationMaster进程中会启动一个线程Driver，进行SparkContext的初始化
4. ApplicationMaster向RM注册，申请计算资源
5. RM返回资源充足的NM信息给AM
6. AM要求NM启动executor
7. executor向Driver反向注册，告知资源已经准备完毕，可以提交任务
8. Driver提交task到executor执行
9. 当所有task执行完毕，ApplicationMaster注销自己，释放资源
# spark submit常用参数

```
spark-submit --class org.apache.spark.examples.SparkPi --master yarn --deploy-mode cluster --driver-memory 8g --num-executors 2 --executor-memory 8g --executor-cores 4 /opt/apps/spark-1.6.0-bin-hadoop2.6/lib/spark-examples*.jar 10
```

|**参数**|**说明**|
|:----|:----|
| master          | 指定资源调度器 local standalone yarn                         |
| deploy mode     | 运行模式client、cluster                                      |
| class           | 作业主类的全类名                                             |
| driver-memory   | 指定Driver的内存大小,默认1G,工作中一般设置为5-10G            |
| executor-memory | 指定每个executor的内存大小                                   |
| driver-memory   | driver使用内核数                                             |
| executor-cores  | 各个Executor使用的并发线程(内核)数目，即每个Executor最大可并发执行的Task数目 |
| num-executors   | 创建Executor的个数                                           |
| quene           | 指定资源队列                                                 |




cluster和client区别

1. YARN-Cluster模式下，Driver运行在AM(Application Master)中，它负责向YARN申请资源，并监督作业的运行状况。当用户提交了作业之后，就可以关掉Client，作业会继续在YARN上运行，因而YARN-Cluster模式不适合运行交互类型的作业
2. YARN-Client模式下，Application Master仅仅向YARN请求Executor，Client会和请求的Container通信来调度他们工作，也就是说Client不能离开
# spark常用算子

## Transformation算子

|**算子**|**解释**|
|:----|:----|
| map          | 映射         |
| mapPartitons | 分区里的映射 |
| flatmap      | 扁平化       |
| filter       | 过滤         |
| distinct     | 去重         |
| repartiton   | 重新分区     |
| sortBy       | 排序         |


## Action算子

|**算子**|**解释**|
|:----|:----|
| reduce  | 聚合                         |
| collect | 以数组形式返回数据集         |
| count   | 返回rdd中元素的个数          |
| first   | 返回rdd中第一个元素          |
| foreach | 遍历rdd中每一个元素          |
| take    | 返回rdd中前n个元素组成的数组 |
| count   |                              |


# 宽窄依赖

宽依赖表示一个父RDD的Partition被多个子rdd的partiton依赖

# rdd、dataframe

