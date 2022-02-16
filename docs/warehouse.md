# 数仓笔记

## 数仓架构选型

数仓分层

| 层                          | 说明               | 作用                                                         |
| --------------------------- | ------------------ | ------------------------------------------------------------ |
| ods（Operation Data Store） | 原始数据层         | 存放原始数据，日志以及业务数据                               |
| dwd(data warehouse detail)  | 明细数据层         | 清洗过期、重复、不完整的数据（空值、脏数据）、脱敏，保存业务事实明细，例如一次下单 |
| dim                         | 维度层             | 对业务事实的描述信息，例如何人、何时、何地                   |
| dws(data warehouse summary) | 数据汇总层（宽表） | 按天进行轻度汇总，减少重复的join                             |
| dwt(data warehouse topic)   | 数据主题层         | 对数据进行累积汇总                                           |
| ads(application data store) | 数据应用层         | 为各种统计报表提供数据                                       |



数据集市



useradd xpx

passwd xpx

vim /etc/sudoers



flume-kafka-flume:kafka起到了削峰缓冲的作用，flume至hdfs在活动时，数据量过大，hdfs压力过大，放在kafka中可以慢慢消费



kylin从dwd层查询数据，存储在HBase

Presto从dwd、dws、dwt层基于内存查询



## linux相关知识

| 常用命令或参数         | 功能                                         | 额外说明                                                     |
| ---------------------- | -------------------------------------------- | ------------------------------------------------------------ |
| nohup                  | 免疫关闭session所发送的SIGHUP信号            | 常与&配合使用                                                |
| &                      | 免疫Ctrl + C发送的SIGINT信号                 | 常与nohup配合使用                                            |
| >/dev/null             | 将标准输出1重定向到`/dev/null`中             | /dev/null代表[linux](https://so.csdn.net/so/search?from=pc_blog_highlight&q=linux)的空设备文件，所有往这个文件里面写入的内容都会丢失，俗称“黑洞” |
| 2>&1                   | 重定向绑定                                   | 错误输出将会和标准输出输出到同一个地方                       |
| ps -ef/aux \|grep 进程 | 查看进程端口号                               |                                                              |
| grep -v                | 过滤不需要的进程                             | 其他参数：-i 忽略大小写 -E支持正则表达式                     |
| \|                     | 上一条命令的标准输出，作为下一条命令标准输入 | 示例：echo "--help" \| cat                                   |
| xargs                  | 上一条命令的标准输出，作为下一条命令的参数   | 示例：echo "--help" \| xargs cat                             |
| awk                    | 文本分析工具                                 | 示例：awk '{print $2 }' 返回分隔符的第二位，默认分隔符为空格，指定使用参数-F |
| ls -a                  | 显示隐藏文件                                 | 可显示后缀为.swp的文件                                       |
| rpm -qa                | 显示所有rpm的安装包                          |                                                              |
| rpm -e –nodeps         | 强制卸载安装包                               |                                                              |
| rpm -ivh               | 安装安装包                                   |                                                              |
| :g/^s*$/d              | 删除空行                                     | g ：全区命令<br/>/ ：分隔符<br/>^s*$ ：匹配空行，其中^表示行首，s表示空字符，包括空格和制表符，*重复0到n个前面的字符，$表示行尾。连起来就是匹配只有空字符的行，也就是空行。<br/>/d ：删除该行 |
| %s/^\s\*//g            | 删除行首空格                                 |                                                              |



vim编辑

`:%s/foo/bar/g`会在全局范围(`%`)查找`foo`并替换为`bar`，所有出现都会被替换（`g`）。

## zookeeper相关知识

zk脚本

```shell
#!/bin/bash
  
case $1 in
"start"){
        for i in xpx101 xpx102 xpx103
        do
                echo =========== zookeeper $i 启动 ============
                ssh $i "/opt/xpx/module/zookeeper-3.5.7-bin/bin/zkServer.sh start"
        done
};;
"stop"){
        for i in xpx101 xpx102 xpx103
        do
                echo =========== zookeeper $i 停止 ============
                ssh $i "/opt/xpx/module/zookeeper-3.5.7-bin/bin/zkServer.sh stop"
        done
};;
"status"){
        for i in xpx101 xpx102 xpx103
        do
                echo =========== zookeeper $i 状态 ============
                ssh $i "/opt/xpx/module/zookeeper-3.5.7-bin/bin/zkServer.sh status"
        done
};;
esac

```



## kafka相关知识

### producer:

ack 0 1 -1

拦截器，序列化器，分区器

发送流程 sender main

幂等性，事务

分区规则：

1. 有指定分区则发往指定分区
2. 没有指定分区，则根据Key值Hash
3. 没有指定分区也没有Key的时候，轮询(粘性)



### broker

 topic:

​	副本：高可靠

​	ISR、LEO、HW

​	分区：高并发（读、写角度），负载均衡（集群角度）（防止热点 ）说明：某topic信息量大，分区到不同服务器上减压



Consumer

​	分区分配规则

​	offset保存

​		默认：__consumer_offset 主题

​		其他：  手动维护Offset(Mysql)  保存数据&保存offset写到一个事务

​		精准一次消费

​		先保存数据后保存Offset 重复数据+幂等性

​		先保存Offset后保存数据 丢失数据



## flume相关知识

### flume常用命令

```shell
#启动
./bin/flume-ng agent --conf-file /opt/xpx/module/flume/conf/file-flume-kafka.conf  --name a1 -Dflume.root.logger=INFO,LOGFILE >/opt/xpx/module/flume/log1.txt 2>&1 &
```



### source

source:

taildirsource优点：支持断点续传，多目录

缺点：文件重命名会重新读取该文件造成重复，原因：taildir_position.json以innode和全路径作为联合主键

注意：

	1. 要使用不更名打印日志框架（logback）
 	2. 修改源码，让TailDirSource判断文件时只看iNode



### 自定义拦截器

阶段：flume至kafka,source至channel

作用：进行ETL清洗，过滤JSON格式不完整的数据

代码

```java
package com.xpx.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

public class ETLInterceptor implements Interceptor {

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {

        byte[] body = event.getBody();
        String log = new String(body, StandardCharsets.UTF_8);

        if (JSONUtils.isJSONValidate(log)) {
            return event;
        } else {
            return null;
        }
    }

    @Override
    public List<Event> intercept(List<Event> list) {

        Iterator<Event> iterator = list.iterator();

        while (iterator.hasNext()){
            Event next = iterator.next();
            if(intercept(next)==null){
                iterator.remove();
            }
        }

        return list;
    }

    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new ETLInterceptor();
        }
        @Override
        public void configure(Context context) {

        }

    }

    @Override
    public void close() {

    }
}
```



```java
package com.xpx.flume.interceptor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;

public class JSONUtils {

//    public static void main(String[] args) {
//        System.out.println(isJSONValidate("{11}"));
//        System.out.println(isJSONValidate("{xixi:1}"));
//    }

    public static boolean isJSONValidate(String log){
        try {
            JSON.parse(log);
            return true;
        }catch (JSONException e){
            return false;
        }
    }
}
```



总结

自定义拦截器步骤

1. 实现Interceptor接口，重写4个方法
2. intercept方法处理单个event
3. Intercept方法处理event集合
4. 调用fastjson里面的json.parse方法判断json格式的完整（也可自己使用string的startwith()和endwith()）
5. 增加静态内部类返回一个拦截器对象



### 时间戳拦截器

阶段：flume至hdfs

作用：解决零点漂移的问题，避免flume上传到hdfs时采用linux默认系统时间产生误差，需要取到日志生成的时间

解决方案：将event的body信息转化为json,获取其中时间戳的值，放入event的headers信息中，headers名字为timestamp

代码

```java
package com.xpx.flume.interceptor;


import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class TimestampInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        Map<String, String> headers = event.getHeaders();
        byte[] body = event.getBody();
        String log = new String(body, StandardCharsets.UTF_8);
        JSONObject jsonObject = JSONObject.parseObject(log);
        String ts = jsonObject.getString("ts");
        headers.put("timestamp",ts);
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        for (Event event : list) {
            intercept(event);
        }
        return list;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new TimestampInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}

```



### 自定义拦截器步骤

1. 实现Interceptor接口，重写4个方法
2. intercept方法处理单个event
3. Intercept方法处理event集合
4. 调用fastjson里面的json.parse方法判断json格式的完整（也可自己使用string的startwith()和endwith()）
5. 增加静态内部类返回一个拦截器对象



## sqoop相关知识

### 配置sqoop环境变量

vim /conf/sqoop-env.sh

```shell
#sqoop底层是mapreduce,sqoop需要hadoop的依赖和配置文件
export HADOOP_COMMON_HOME=/opt/xpx/module/hadoop-3.1.3
export HADOOP_MAPRED_HOME=/opt/xpx/module/hadoop-3.1.3
#sqoop可以将关系型数据库（mysql）的数据上传到hdfs的目录下或者hive、hbase具体的表中
export HIVE_HOME=/opt/xpx/module/hive
#往hbase读写文件必须和zookeeper进行交互
export ZOOKEEPER_HOME=/opt/xpx/module/zookeeper-3.5.7
export ZOOCFGDIR=/opt/xpx/module/zookeeper-3.5.7/conf
```



常用命令

```shell
#测试连接
bin/sqoop list-databases --connect jdbc:mysql://xpx101:3306/ --username root --password 123456
#help
bin/sqoop help
```



### sqoop测试

形势1：

```shell
bin/sqoop import \
--connect jdbc:mysql://xpx101:3306/gmall \
--username root \
--password 123456 \
--table user_info \
--columns id,login_name \
--where 'id>=10 and id<=30' \
--target-dir /cp22 \
--delete-target-dir \
--fields-terminated-by '\t' \
--num-mappers 2 \
--driver com.mysql.jdbc.Driver \
--split-by id
```

注：加“ ”是因为有多个空格会识别为多个参数



形势2（sql）:

```shell
bin/sqoop import \
--connect jdbc:mysql://xpx101:3306/gmall \
--username root \
--password 123456 \
--query 'select id,order_status from order_info where left(create_time,10)='2020-06-15' and $CONDITIONS' \
--target-dir /cp66 \
--delete-target-dir \
--fields-terminated-by '\t' \
--num-mappers 2 \
--driver com.mysql.jdbc.Driver \
--split-by id
```



```shell
bin/sqoop import \
--connect jdbc:mysql://xpx101:3306/gmall \
--username root \
--password 123456 \
--query 'select id,order_status from order_info where date_format(create_time,'%Y-%m-%d')='2020-06-15' and $CONDITIONS' \
--target-dir /cp66 \
--delete-target-dir \
--fields-terminated-by '\t' \
--num-mappers 2 \
--driver com.mysql.jdbc.Driver \
--split-by id
```

注：加$CONDITIONS是给不同的maptask限定条件，这里就id<20



sqoop首日同步

```shell
#! /bin/bash

APP=gmall
sqoop=/opt/module/sqoop/bin/sqoop

if [ -n "$2" ] ;then
   do_date=$2
else 
   echo "请传入日期参数"
   exit
fi 

import_data(){
$sqoop import \
--connect jdbc:mysql://hadoop102:3306/$APP \
--username root \
--password 000000 \
--target-dir /origin_data/$APP/db/$1/$do_date \
--delete-target-dir \
--query "$2 where \$CONDITIONS" \
--num-mappers 1 \
--fields-terminated-by '\t' \
--compress \
--compression-codec lzop \
--null-string '\\N' \
--null-non-string '\\N'

hadoop jar /opt/module/hadoop-3.1.3/share/hadoop/common/hadoop-lzo-0.4.20.jar com.hadoop.compression.lzo.DistributedLzoIndexer /origin_data/$APP/db/$1/$do_date
}

#null-string 的作用是将mysql中的null值转化为/N，hive中的空值为/N，这样操作可以使hive识别
#hadoop jar 是为lzop创建索引，lzop只有创建了索引才能支持切片
```





```shell
#! /bin/bash

APP=gmall
sqoop=/opt/module/sqoop/bin/sqoop

if [ -n "$2" ] ;then
   do_date=$2
else 
   echo "请传入日期参数"
   exit
fi 

import_data(){
$sqoop import \
--connect jdbc:mysql://xpx101:3306/$APP \
--username root \
--password 123456 \
--target-dir /origin_data/$APP/db/$1/$do_date \
--delete-target-dir \
--query "$2 where \$CONDITIONS" \
--num-mappers 1 \
--fields-terminated-by '\t' \
--compress \
--compression-codec lzop \
--driver com.mysql.jdbc.Driver \
--null-string '\\N' \
--null-non-string '\\N'

hadoop jar /opt/xpx/module/hadoop-3.1.3/share/hadoop/common/hadoop-lzo-0.4.20.jar com.hadoop.compression.lzo.DistributedLzoIndexer /origin_data/$APP/db/$1/$do_date
}

import_order_info(){
  import_data order_info "select
                            id, 
                            total_amount, 
                            order_status, 
                            user_id, 
                            payment_way,
                            delivery_address,
                            out_trade_no, 
                            create_time, 
                            operate_time,
                            expire_time,
                            tracking_no,
                            province_id,
                            activity_reduce_amount,
                            coupon_reduce_amount,                            
                            original_total_amount,
                            feight_fee,
                            feight_fee_reduce      
                        from order_info"
}

import_coupon_use(){
  import_data coupon_use "select
                          id,
                          coupon_id,
                          user_id,
                          order_id,
                          coupon_status,
                          get_time,
                          using_time,
                          used_time,
                          expire_time
                        from coupon_use"
}

import_order_status_log(){
  import_data order_status_log "select
                                  id,
                                  order_id,
                                  order_status,
                                  operate_time
                                from order_status_log"
}

import_user_info(){
  import_data "user_info" "select 
                            id,
                            login_name,
                            nick_name,
                            name,
                            phone_num,
                            email,
                            user_level, 
                            birthday,
                            gender,
                            create_time,
                            operate_time
                          from user_info"
}

import_order_detail(){
  import_data order_detail "select 
                              id,
                              order_id, 
                              sku_id,
                              sku_name,
                              order_price,
                              sku_num, 
                              create_time,
                              source_type,
                              source_id,
                              split_total_amount,
                              split_activity_amount,
                              split_coupon_amount
                            from order_detail"
}

import_payment_info(){
  import_data "payment_info"  "select 
                                id,  
                                out_trade_no, 
                                order_id, 
                                user_id, 
                                payment_type, 
                                trade_no, 
                                total_amount,  
                                subject, 
                                payment_status,
                                create_time,
                                callback_time 
                              from payment_info"
}

import_comment_info(){
  import_data comment_info "select
                              id,
                              user_id,
                              sku_id,
                              spu_id,
                              order_id,
                              appraise,
                              create_time
                            from comment_info"
}

import_order_refund_info(){
  import_data order_refund_info "select
                                id,
                                user_id,
                                order_id,
                                sku_id,
                                refund_type,
                                refund_num,
                                refund_amount,
                                refund_reason_type,
                                refund_status,
                                create_time
                              from order_refund_info"
}

import_sku_info(){
  import_data sku_info "select 
                          id,
                          spu_id,
                          price,
                          sku_name,
                          sku_desc,
                          weight,
                          tm_id,
                          category3_id,
                          is_sale,
                          create_time
                        from sku_info"
}

import_base_category1(){
  import_data "base_category1" "select 
                                  id,
                                  name 
                                from base_category1"
}

import_base_category2(){
  import_data "base_category2" "select
                                  id,
                                  name,
                                  category1_id 
                                from base_category2"
}

import_base_category3(){
  import_data "base_category3" "select
                                  id,
                                  name,
                                  category2_id
                                from base_category3"
}

import_base_province(){
  import_data base_province "select
                              id,
                              name,
                              region_id,
                              area_code,
                              iso_code,
                              iso_3166_2
                            from base_province"
}

import_base_region(){
  import_data base_region "select
                              id,
                              region_name
                            from base_region"
}

import_base_trademark(){
  import_data base_trademark "select
                                id,
                                tm_name
                              from base_trademark"
}

import_spu_info(){
  import_data spu_info "select
                            id,
                            spu_name,
                            category3_id,
                            tm_id
                          from spu_info"
}

import_favor_info(){
  import_data favor_info "select
                          id,
                          user_id,
                          sku_id,
                          spu_id,
                          is_cancel,
                          create_time,
                          cancel_time
                        from favor_info"
}

import_cart_info(){
  import_data cart_info "select
                        id,
                        user_id,
                        sku_id,
                        cart_price,
                        sku_num,
                        sku_name,
                        create_time,
                        operate_time,
                        is_ordered,
                        order_time,
                        source_type,
                        source_id
                      from cart_info"
}

import_coupon_info(){
  import_data coupon_info "select
                          id,
                          coupon_name,
                          coupon_type,
                          condition_amount,
                          condition_num,
                          activity_id,
                          benefit_amount,
                          benefit_discount,
                          create_time,
                          range_type,
                          limit_num,
                          taken_count,
                          start_time,
                          end_time,
                          operate_time,
                          expire_time
                        from coupon_info"
}

import_activity_info(){
  import_data activity_info "select
                              id,
                              activity_name,
                              activity_type,
                              start_time,
                              end_time,
                              create_time
                            from activity_info"
}

import_activity_rule(){
    import_data activity_rule "select
                                    id,
                                    activity_id,
                                    activity_type,
                                    condition_amount,
                                    condition_num,
                                    benefit_amount,
                                    benefit_discount,
                                    benefit_level
                                from activity_rule"
}

import_base_dic(){
    import_data base_dic "select
                            dic_code,
                            dic_name,
                            parent_code,
                            create_time,
                            operate_time
                          from base_dic"
}


import_order_detail_activity(){
    import_data order_detail_activity "select
                                                                id,
                                                                order_id,
                                                                order_detail_id,
                                                                activity_id,
                                                                activity_rule_id,
                                                                sku_id,
                                                                create_time
                                                            from order_detail_activity"
}


import_order_detail_coupon(){
    import_data order_detail_coupon "select
                                                                id,
								                                                order_id,
                                                                order_detail_id,
                                                                coupon_id,
                                                                coupon_use_id,
                                                                sku_id,
                                                                create_time
                                                            from order_detail_coupon"
}


import_refund_payment(){
    import_data refund_payment "select
                                                        id,
                                                        out_trade_no,
                                                        order_id,
                                                        sku_id,
                                                        payment_type,
                                                        trade_no,
                                                        total_amount,
                                                        subject,
                                                        refund_status,
                                                        create_time,
                                                        callback_time
                                                    from refund_payment"                                                    

}

import_sku_attr_value(){
    import_data sku_attr_value "select
                                                    id,
                                                    attr_id,
                                                    value_id,
                                                    sku_id,
                                                    attr_name,
                                                    value_name
                                                from sku_attr_value"
}


import_sku_sale_attr_value(){
    import_data sku_sale_attr_value "select
                                                            id,
                                                            sku_id,
                                                            spu_id,
                                                            sale_attr_value_id,
                                                            sale_attr_id,
                                                            sale_attr_name,
                                                            sale_attr_value_name
                                                        from sku_sale_attr_value"
}

case $1 in
  "order_info")
     import_order_info
;;
  "base_category1")
     import_base_category1
;;
  "base_category2")
     import_base_category2
;;
  "base_category3")
     import_base_category3
;;
  "order_detail")
     import_order_detail
;;
  "sku_info")
     import_sku_info
;;
  "user_info")
     import_user_info
;;
  "payment_info")
     import_payment_info
;;
  "base_province")
     import_base_province
;;
  "base_region")
     import_base_region
;;
  "base_trademark")
     import_base_trademark
;;
  "activity_info")
      import_activity_info
;;
  "cart_info")
      import_cart_info
;;
  "comment_info")
      import_comment_info
;;
  "coupon_info")
      import_coupon_info
;;
  "coupon_use")
      import_coupon_use
;;
  "favor_info")
      import_favor_info
;;
  "order_refund_info")
      import_order_refund_info
;;
  "order_status_log")
      import_order_status_log
;;
  "spu_info")
      import_spu_info
;;
  "activity_rule")
      import_activity_rule
;;
  "base_dic")
      import_base_dic
;;
  "order_detail_activity")
      import_order_detail_activity
;;
  "order_detail_coupon")
      import_order_detail_coupon
;;
  "refund_payment")
      import_refund_payment
;;
  "sku_attr_value")
      import_sku_attr_value
;;
  "sku_sale_attr_value")
      import_sku_sale_attr_value
;;
  "all")
   import_base_category1
   import_base_category2
   import_base_category3
   import_order_info
   import_order_detail
   import_sku_info
   import_user_info
   import_payment_info
   import_base_region
   import_base_province
   import_base_trademark
   import_activity_info
   import_cart_info
   import_comment_info
   import_coupon_use
   import_coupon_info
   import_favor_info
   import_order_refund_info
   import_order_status_log
   import_spu_info
   import_activity_rule
   import_base_dic
   import_order_detail_activity
   import_order_detail_coupon
   import_refund_payment
   import_sku_attr_value
   import_sku_sale_attr_value
;;
esac

```



## hadoop相关知识

基础命令

```shell
#递归查询目录下所有文件
hadoop dfs -ls -R /cp3
#查看文件内容
hadoop dfs -cat /cp3/part-m-00001
```



### LZO索引

原因：因为LZO的可切片特性依赖于索引，所以我们需要手动的为LZO压缩文件创建索引

```shell
hadoop jar /opt/module/hadoop-3.1.3/share/hadoop/common/hadoop-lzo-0.4.20.jar com.hadoop.compression.lzo.DistributedLzoIndexer /warehouse/gmall/ods/ods_log/dt=2020-06-14
```



## 集群建模

### 三范式理论

1. 1NF:属性不可切割，字段
2. 2NF:不能存在部分函数依赖
3. 3NF:不能存在传递函数依赖



部分函数依赖：（A,C）->B 且A->B  A、B、C在同一个表中为部分函数依赖

传递函数依赖：A->B A->C B->C A,B,C在同一个表中



关系建模：关系模型严格遵循第三范式（3NF），数据冗余程度低，数据的一致性容易得到保证。由于数据分布于众多的表中，查询会相对复杂，在大数据的场景下，查询效率相对较低



维度建模：维度模型以数据分析作为出发点，不遵循三范式，故数据存在一定的冗余。维度模型面向业务，将业务用事实表和维度表呈现出来。表结构简单，故查询简单，查询效率较高



维度建模

事实表：事实表中的每行数据代表一个业务事件（下单、支付、退款、评价等），包括维度外键和度量值

维度表：一般是对事实的描述信息



雪花模型：维度表拆分的更细，维度表的拆分类似遵循3NF

星型模型：事实表直接关联维度表，不会有多余的分支

## 集群优化

### hdfs小文件问题

小文件的危害

- 存储：占用namenode内存，每个小文件会占用150个字节
- 计算：默认切片规则为一个文件切一次，一个小文件对应一个maptask,一个maptask占用一个内存



①在flume的配置文件中配置参数

| 参数              | 解释                                                         |
| ----------------- | ------------------------------------------------------------ |
| hdfs.rollInterval | 文件创建超xxx秒时会滚动生成新文件                            |
| hdfs.rollSize     | 文件超过xxxM时会滚动生成新文件                               |
| hdfs.rollCount    | event个数达到xxx时会滚动生成新的文件（设置为0则永远不根据事件的数量滚动） |



vim kafka-flume-hdfs.conf

```shell
## 组件
a1.sources=r1
a1.channels=c1
a1.sinks=k1

## source1
a1.sources.r1.type = org.apache.flume.source.kafka.KafkaSource
a1.sources.r1.batchSize = 5000
a1.sources.r1.batchDurationMillis = 2000
a1.sources.r1.kafka.bootstrap.servers = xpx101:9092,xpx102:9092,xpx103:9092
a1.sources.r1.kafka.topics=haha
a1.sources.r1.interceptors = i1
a1.sources.r1.interceptors.i1.type = com.xpx.flume.interceptor.TimeStampInterceptor$Builder

## channel1
a1.channels.c1.type = file
#磁盘数据在内存中索引落盘的位置
a1.channels.c1.checkpointDir = /opt/xpx/module/flume/checkpoint/behavior1
#数据备份的磁盘位置
a1.channels.c1.dataDirs = /opt/xpx/module/flume/data/behavior1/


## sink1
a1.sinks.k1.type = hdfs
a1.sinks.k1.hdfs.path = /origin_data/gmall/log/topic_log/%Y-%m-%d
#前缀
a1.sinks.k1.hdfs.filePrefix = log-
a1.sinks.k1.hdfs.round = false

#控制生成的小文件
a1.sinks.k1.hdfs.rollInterval = 10
a1.sinks.k1.hdfs.rollSize = 134217728
a1.sinks.k1.hdfs.rollCount = 0

## 控制输出文件是原生文件。
a1.sinks.k1.hdfs.fileType = CompressedStream
a1.sinks.k1.hdfs.codeC = lzop

## 拼装
a1.sources.r1.channels = c1
a1.sinks.k1.channel= c1

```



②设置 set hive.input.format 

hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;（在map端合并小文件）

但是在有LZO索引文件时会导致LZO索引文件丢失，此时需要取消合并小文件

set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

## 日志数据

```shell
#!/bin/bash

# 定义变量方便修改
APP=gmall

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
   do_date=$1
else 
   do_date=`date -d "-1 day" +%F`
fi 

echo ================== 日志日期为 $do_date ==================
sql="
load data inpath '/origin_data/$APP/log/topic_log/$do_date' into table ${APP}.ods_log partition(dt='$do_date');
"

hive -e "$sql"

hadoop jar /opt/module/hadoop-3.1.3/share/hadoop/common/hadoop-lzo-0.4.20.jar com.hadoop.compression.lzo.DistributedLzoIndexer /warehouse/$APP/ods/ods_log/dt=$do_date

```



## 业务数据

### 业务表设计

| 表名                  | 中文名             | 说明                                           |
| --------------------- | ------------------ | ---------------------------------------------- |
| base_attr_info        | 平台属性表         | 类似CPU、操作系统，注意和SKU表的区别           |
| base_attr_value       | 平台属性值表       | 具体的CPU型号和操作系统名称                    |
| base_dic              | 字典表             | 存取key,value 比如order_status = 1001 为已发货 |
| order_detail_activity | 订单明细活动关联表 | 使用明细是因为3个SKU可能只有两个SKU参与了活动  |
| order_status_log      | 订单状态流水表     | 记录了订单状态变化的明细表                     |
| sku_attr_value        | SKU平台属性表      | 记录该SKU对应哪些平台属性值                    |



### mysql杂记

mysql启动

```shell
#启动mysql
systemctl start mysqld
#查看mysql状态
systemctl status mysqld
```



首日装载

```shell
#!/bin/bash

APP=gmall

if [ -n "$2" ] ;then
   do_date=$2
else 
   echo "请传入日期参数"
   exit
fi 

ods_order_info=" 
load data inpath '/origin_data/$APP/db/order_info/$do_date' OVERWRITE into table ${APP}.ods_order_info partition(dt='$do_date');"

ods_order_detail="
load data inpath '/origin_data/$APP/db/order_detail/$do_date' OVERWRITE into table ${APP}.ods_order_detail partition(dt='$do_date');"

ods_sku_info="
load data inpath '/origin_data/$APP/db/sku_info/$do_date' OVERWRITE into table ${APP}.ods_sku_info partition(dt='$do_date');"

ods_user_info="
load data inpath '/origin_data/$APP/db/user_info/$do_date' OVERWRITE into table ${APP}.ods_user_info partition(dt='$do_date');"

ods_payment_info="
load data inpath '/origin_data/$APP/db/payment_info/$do_date' OVERWRITE into table ${APP}.ods_payment_info partition(dt='$do_date');"

ods_base_category1="
load data inpath '/origin_data/$APP/db/base_category1/$do_date' OVERWRITE into table ${APP}.ods_base_category1 partition(dt='$do_date');"

ods_base_category2="
load data inpath '/origin_data/$APP/db/base_category2/$do_date' OVERWRITE into table ${APP}.ods_base_category2 partition(dt='$do_date');"

ods_base_category3="
load data inpath '/origin_data/$APP/db/base_category3/$do_date' OVERWRITE into table ${APP}.ods_base_category3 partition(dt='$do_date'); "

ods_base_trademark="
load data inpath '/origin_data/$APP/db/base_trademark/$do_date' OVERWRITE into table ${APP}.ods_base_trademark partition(dt='$do_date'); "

ods_activity_info="
load data inpath '/origin_data/$APP/db/activity_info/$do_date' OVERWRITE into table ${APP}.ods_activity_info partition(dt='$do_date'); "

ods_cart_info="
load data inpath '/origin_data/$APP/db/cart_info/$do_date' OVERWRITE into table ${APP}.ods_cart_info partition(dt='$do_date'); "

ods_comment_info="
load data inpath '/origin_data/$APP/db/comment_info/$do_date' OVERWRITE into table ${APP}.ods_comment_info partition(dt='$do_date'); "

ods_coupon_info="
load data inpath '/origin_data/$APP/db/coupon_info/$do_date' OVERWRITE into table ${APP}.ods_coupon_info partition(dt='$do_date'); "

ods_coupon_use="
load data inpath '/origin_data/$APP/db/coupon_use/$do_date' OVERWRITE into table ${APP}.ods_coupon_use partition(dt='$do_date'); "

ods_favor_info="
load data inpath '/origin_data/$APP/db/favor_info/$do_date' OVERWRITE into table ${APP}.ods_favor_info partition(dt='$do_date'); "

ods_order_refund_info="
load data inpath '/origin_data/$APP/db/order_refund_info/$do_date' OVERWRITE into table ${APP}.ods_order_refund_info partition(dt='$do_date'); "

ods_order_status_log="
load data inpath '/origin_data/$APP/db/order_status_log/$do_date' OVERWRITE into table ${APP}.ods_order_status_log partition(dt='$do_date'); "

ods_spu_info="
load data inpath '/origin_data/$APP/db/spu_info/$do_date' OVERWRITE into table ${APP}.ods_spu_info partition(dt='$do_date'); "

ods_activity_rule="
load data inpath '/origin_data/$APP/db/activity_rule/$do_date' OVERWRITE into table ${APP}.ods_activity_rule partition(dt='$do_date');" 

ods_base_dic="
load data inpath '/origin_data/$APP/db/base_dic/$do_date' OVERWRITE into table ${APP}.ods_base_dic partition(dt='$do_date'); "

ods_order_detail_activity="
load data inpath '/origin_data/$APP/db/order_detail_activity/$do_date' OVERWRITE into table ${APP}.ods_order_detail_activity partition(dt='$do_date'); "

ods_order_detail_coupon="
load data inpath '/origin_data/$APP/db/order_detail_coupon/$do_date' OVERWRITE into table ${APP}.ods_order_detail_coupon partition(dt='$do_date'); "

ods_refund_payment="
load data inpath '/origin_data/$APP/db/refund_payment/$do_date' OVERWRITE into table ${APP}.ods_refund_payment partition(dt='$do_date'); "

ods_sku_attr_value="
load data inpath '/origin_data/$APP/db/sku_attr_value/$do_date' OVERWRITE into table ${APP}.ods_sku_attr_value partition(dt='$do_date'); "

ods_sku_sale_attr_value="
load data inpath '/origin_data/$APP/db/sku_sale_attr_value/$do_date' OVERWRITE into table ${APP}.ods_sku_sale_attr_value partition(dt='$do_date'); "

ods_base_province=" 
load data inpath '/origin_data/$APP/db/base_province/$do_date' OVERWRITE into table ${APP}.ods_base_province;"

ods_base_region="
load data inpath '/origin_data/$APP/db/base_region/$do_date' OVERWRITE into table ${APP}.ods_base_region;"

case $1 in
    "ods_order_info"){
        hive -e "$ods_order_info"
    };;
    "ods_order_detail"){
        hive -e "$ods_order_detail"
    };;
    "ods_sku_info"){
        hive -e "$ods_sku_info"
    };;
    "ods_user_info"){
        hive -e "$ods_user_info"
    };;
    "ods_payment_info"){
        hive -e "$ods_payment_info"
    };;
    "ods_base_category1"){
        hive -e "$ods_base_category1"
    };;
    "ods_base_category2"){
        hive -e "$ods_base_category2"
    };;
    "ods_base_category3"){
        hive -e "$ods_base_category3"
    };;
    "ods_base_trademark"){
        hive -e "$ods_base_trademark"
    };;
    "ods_activity_info"){
        hive -e "$ods_activity_info"
    };;
    "ods_cart_info"){
        hive -e "$ods_cart_info"
    };;
    "ods_comment_info"){
        hive -e "$ods_comment_info"
    };;
    "ods_coupon_info"){
        hive -e "$ods_coupon_info"
    };;
    "ods_coupon_use"){
        hive -e "$ods_coupon_use"
    };;
    "ods_favor_info"){
        hive -e "$ods_favor_info"
    };;
    "ods_order_refund_info"){
        hive -e "$ods_order_refund_info"
    };;
    "ods_order_status_log"){
        hive -e "$ods_order_status_log"
    };;
    "ods_spu_info"){
        hive -e "$ods_spu_info"
    };;
    "ods_activity_rule"){
        hive -e "$ods_activity_rule"
    };;
    "ods_base_dic"){
        hive -e "$ods_base_dic"
    };;
    "ods_order_detail_activity"){
        hive -e "$ods_order_detail_activity"
    };;
    "ods_order_detail_coupon"){
        hive -e "$ods_order_detail_coupon"
    };;
    "ods_refund_payment"){
        hive -e "$ods_refund_payment"
    };;
    "ods_sku_attr_value"){
        hive -e "$ods_sku_attr_value"
    };;
    "ods_sku_sale_attr_value"){
        hive -e "$ods_sku_sale_attr_value"
    };;
    "ods_base_province"){
        hive -e "$ods_base_province"
    };;
    "ods_base_region"){
        hive -e "$ods_base_region"
    };;
    "all"){
        hive -e "$ods_order_info$ods_order_detail$ods_sku_info$ods_user_info$ods_payment_info$ods_base_category1$ods_base_category2$ods_base_category3$ods_base_trademark$ods_activity_info$ods_cart_info$ods_comment_info$ods_coupon_info$ods_coupon_use$ods_favor_info$ods_order_refund_info$ods_order_status_log$ods_spu_info$ods_activity_rule$ods_base_dic$ods_order_detail_activity$ods_order_detail_coupon$ods_refund_payment$ods_sku_attr_value$ods_sku_sale_attr_value$ods_base_province$ods_base_region"
    };;
esac

```



## hive相关知识

### hive常用函数

```sql
-- named_struct 封装为一个结构体
-- collect_set 集合接收这些结构体
select
    sku_id,
    collect_set(named_struct('attr_id',attr_id,'attr_name',attr_name,'value_id',value_id,'value_name',value_name))
from ods_sku_attr_value
where dt = '2020-06-14'
group by sku_id
```



### 常用命令

```sql
-- 删除分区表数据
alter table ${APP}.dwt_visitor_topic drop partition(dt='$clear_date');
-- 外部表只删除元数据，还需要删除hdfs上的数据
hadoop fs -rm -r -f /warehouse/gmall/dwt/dwt_area_topic/dt=$clear_date
```





### hive相关参数

```sql
-- 设施动态分区
set hive.exec.dynamic.partition.mode=nonstrict;
-- 合并小文件
hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat
-- 解决lzo索引文件失效导致异常值
hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat
```



# sublime使用技巧

行尾加逗号

1. `Ctrl+A`
2. `Ctrl+Shift+L`
3. 使用左右方向键（←或者→）



# 数仓分区

## dim层



### dim层首日脚本

```shell
#!/bin/bash

APP=gmall

if [ -n "$2" ] ;then
   do_date=$2
else 
   echo "请传入日期参数"
   exit
fi 

dim_user_info="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dim_user_info partition(dt='9999-99-99')
select
    id,
    login_name,
    nick_name,
    md5(name),
    md5(phone_num),
    md5(email),
    user_level,
    birthday,
    gender,
    create_time,
    operate_time,
    '$do_date',
    '9999-99-99'
from ${APP}.ods_user_info
where dt='$do_date';
"

dim_sku_info="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
with
sku as
(
    select
        id,
        price,
        sku_name,
        sku_desc,
        weight,
        is_sale,
        spu_id,
        category3_id,
        tm_id,
        create_time
    from ${APP}.ods_sku_info
    where dt='$do_date'
),
spu as
(
    select
        id,
        spu_name
    from ${APP}.ods_spu_info
    where dt='$do_date'
),
c3 as
(
    select
        id,
        name,
        category2_id
    from ${APP}.ods_base_category3
    where dt='$do_date'
),
c2 as
(
    select
        id,
        name,
        category1_id
    from ${APP}.ods_base_category2
    where dt='$do_date'
),
c1 as
(
    select
        id,
        name
    from ${APP}.ods_base_category1
    where dt='$do_date'
),
tm as
(
    select
        id,
        tm_name
    from ${APP}.ods_base_trademark
    where dt='$do_date'
),
attr as
(
    select
        sku_id,
        collect_set(named_struct('attr_id',attr_id,'value_id',value_id,'attr_name',attr_name,'value_name',value_name)) attrs
    from ${APP}.ods_sku_attr_value
    where dt='$do_date'
    group by sku_id
),
sale_attr as
(
    select
        sku_id,
        collect_set(named_struct('sale_attr_id',sale_attr_id,'sale_attr_value_id',sale_attr_value_id,'sale_attr_name',sale_attr_name,'sale_attr_value_name',sale_attr_value_name)) sale_attrs
    from ${APP}.ods_sku_sale_attr_value
    where dt='$do_date'
    group by sku_id
)

insert overwrite table ${APP}.dim_sku_info partition(dt='$do_date')
select
    sku.id,
    sku.price,
    sku.sku_name,
    sku.sku_desc,
    sku.weight,
    sku.is_sale,
    sku.spu_id,
    spu.spu_name,
    sku.category3_id,
    c3.name,
    c3.category2_id,
    c2.name,
    c2.category1_id,
    c1.name,
    sku.tm_id,
    tm.tm_name,
    attr.attrs,
    sale_attr.sale_attrs,
    sku.create_time
from sku
left join spu on sku.spu_id=spu.id
left join c3 on sku.category3_id=c3.id
left join c2 on c3.category2_id=c2.id
left join c1 on c2.category1_id=c1.id
left join tm on sku.tm_id=tm.id
left join attr on sku.id=attr.sku_id
left join sale_attr on sku.id=sale_attr.sku_id;
"

dim_base_province="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dim_base_province
select
    bp.id,
    bp.name,
    bp.area_code,
    bp.iso_code,
    bp.iso_3166_2,
    bp.region_id,
    br.region_name
from ${APP}.ods_base_province bp
join ${APP}.ods_base_region br on bp.region_id = br.id;
"

dim_coupon_info="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dim_coupon_info partition(dt='$do_date')
select
    id,
    coupon_name,
    coupon_type,
    condition_amount,
    condition_num,
    activity_id,
    benefit_amount,
    benefit_discount,
    create_time,
    range_type,
    limit_num,
    taken_count,
    start_time,
    end_time,
    operate_time,
    expire_time
from ${APP}.ods_coupon_info
where dt='$do_date';
"

dim_activity_rule_info="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dim_activity_rule_info partition(dt='$do_date')
select
    ar.id,
    ar.activity_id,
    ai.activity_name,
    ar.activity_type,
    ai.start_time,
    ai.end_time,
    ai.create_time,
    ar.condition_amount,
    ar.condition_num,
    ar.benefit_amount,
    ar.benefit_discount,
    ar.benefit_level
from
(
    select
        id,
        activity_id,
        activity_type,
        condition_amount,
        condition_num,
        benefit_amount,
        benefit_discount,
        benefit_level
    from ${APP}.ods_activity_rule
    where dt='$do_date'
)ar
left join
(
    select
        id,
        activity_name,
        start_time,
        end_time,
        create_time
    from ${APP}.ods_activity_info
    where dt='$do_date'
)ai
on ar.activity_id=ai.id;
"

case $1 in
"dim_user_info"){
    hive -e "$dim_user_info"
};;
"dim_sku_info"){
    hive -e "$dim_sku_info"
};;
"dim_base_province"){
    hive -e "$dim_base_province"
};;
"dim_coupon_info"){
    hive -e "$dim_coupon_info"
};;
"dim_activity_rule_info"){
    hive -e "$dim_activity_rule_info"
};;
"all"){
    hive -e "$dim_user_info$dim_sku_info$dim_coupon_info$dim_activity_rule_info$dim_base_province"
};;
esac

```



### dim 每日脚本

```shell
#!/bin/bash

APP=gmall

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$2" ] ;then
    do_date=$2
else 
    do_date=`date -d "-1 day" +%F`
fi

dim_user_info="
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
with
tmp as
(
    select
        old.id old_id,
        old.login_name old_login_name,
        old.nick_name old_nick_name,
        old.name old_name,
        old.phone_num old_phone_num,
        old.email old_email,
        old.user_level old_user_level,
        old.birthday old_birthday,
        old.gender old_gender,
        old.create_time old_create_time,
        old.operate_time old_operate_time,
        old.start_date old_start_date,
        old.end_date old_end_date,
        new.id new_id,
        new.login_name new_login_name,
        new.nick_name new_nick_name,
        new.name new_name,
        new.phone_num new_phone_num,
        new.email new_email,
        new.user_level new_user_level,
        new.birthday new_birthday,
        new.gender new_gender,
        new.create_time new_create_time,
        new.operate_time new_operate_time,
        new.start_date new_start_date,
        new.end_date new_end_date
    from
    (
        select
            id,
            login_name,
            nick_name,
            name,
            phone_num,
            email,
            user_level,
            birthday,
            gender,
            create_time,
            operate_time,
            start_date,
            end_date
        from ${APP}.dim_user_info
        where dt='9999-99-99'
        and start_date<'$do_date'
    )old
    full outer join
    (
        select
            id,
            login_name,
            nick_name,
            md5(name) name,
            md5(phone_num) phone_num,
            md5(email) email,
            user_level,
            birthday,
            gender,
            create_time,
            operate_time,
            '$do_date' start_date,
            '9999-99-99' end_date
        from ${APP}.ods_user_info
        where dt='$do_date'
    )new
    on old.id=new.id
)
insert overwrite table ${APP}.dim_user_info partition(dt)
select
    nvl(new_id,old_id),
    nvl(new_login_name,old_login_name),
    nvl(new_nick_name,old_nick_name),
    nvl(new_name,old_name),
    nvl(new_phone_num,old_phone_num),
    nvl(new_email,old_email),
    nvl(new_user_level,old_user_level),
    nvl(new_birthday,old_birthday),
    nvl(new_gender,old_gender),
    nvl(new_create_time,old_create_time),
    nvl(new_operate_time,old_operate_time),
    nvl(new_start_date,old_start_date),
    nvl(new_end_date,old_end_date),
    nvl(new_end_date,old_end_date) dt
from tmp
union all
select
    old_id,
    old_login_name,
    old_nick_name,
    old_name,
    old_phone_num,
    old_email,
    old_user_level,
    old_birthday,
    old_gender,
    old_create_time,
    old_operate_time,
    old_start_date,
    cast(date_add('$do_date',-1) as string),
    cast(date_add('$do_date',-1) as string) dt
from tmp
where new_id is not null and old_id is not null;
"

dim_sku_info="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
with
sku as
(
    select
        id,
        price,
        sku_name,
        sku_desc,
        weight,
        is_sale,
        spu_id,
        category3_id,
        tm_id,
        create_time
    from ${APP}.ods_sku_info
    where dt='$do_date'
),
spu as
(
    select
        id,
        spu_name
    from ${APP}.ods_spu_info
    where dt='$do_date'
),
c3 as
(
    select
        id,
        name,
        category2_id
    from ${APP}.ods_base_category3
    where dt='$do_date'
),
c2 as
(
    select
        id,
        name,
        category1_id
    from ${APP}.ods_base_category2
    where dt='$do_date'
),
c1 as
(
    select
        id,
        name
    from ${APP}.ods_base_category1
    where dt='$do_date'
),
tm as
(
    select
        id,
        tm_name
    from ${APP}.ods_base_trademark
    where dt='$do_date'
),
attr as
(
    select
        sku_id,
        collect_set(named_struct('attr_id',attr_id,'value_id',value_id,'attr_name',attr_name,'value_name',value_name)) attrs
    from ${APP}.ods_sku_attr_value
    where dt='$do_date'
    group by sku_id
),
sale_attr as
(
    select
        sku_id,
        collect_set(named_struct('sale_attr_id',sale_attr_id,'sale_attr_value_id',sale_attr_value_id,'sale_attr_name',sale_attr_name,'sale_attr_value_name',sale_attr_value_name)) sale_attrs
    from ${APP}.ods_sku_sale_attr_value
    where dt='$do_date'
    group by sku_id
)

insert overwrite table ${APP}.dim_sku_info partition(dt='$do_date')
select
    sku.id,
    sku.price,
    sku.sku_name,
    sku.sku_desc,
    sku.weight,
    sku.is_sale,
    sku.spu_id,
    spu.spu_name,
    sku.category3_id,
    c3.name,
    c3.category2_id,
    c2.name,
    c2.category1_id,
    c1.name,
    sku.tm_id,
    tm.tm_name,
    attr.attrs,
    sale_attr.sale_attrs,
    sku.create_time
from sku
left join spu on sku.spu_id=spu.id
left join c3 on sku.category3_id=c3.id
left join c2 on c3.category2_id=c2.id
left join c1 on c2.category1_id=c1.id
left join tm on sku.tm_id=tm.id
left join attr on sku.id=attr.sku_id
left join sale_attr on sku.id=sale_attr.sku_id;
"

dim_base_province="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dim_base_province
select
    bp.id,
    bp.name,
    bp.area_code,
    bp.iso_code,
    bp.iso_3166_2,
    bp.region_id,
    bp.name
from ${APP}.ods_base_province bp
join ${APP}.ods_base_region br on bp.region_id = br.id;
"

dim_coupon_info="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dim_coupon_info partition(dt='$do_date')
select
    id,
    coupon_name,
    coupon_type,
    condition_amount,
    condition_num,
    activity_id,
    benefit_amount,
    benefit_discount,
    create_time,
    range_type,
    limit_num,
    taken_count,
    start_time,
    end_time,
    operate_time,
    expire_time
from ${APP}.ods_coupon_info
where dt='$do_date';
"

dim_activity_rule_info="
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
insert overwrite table ${APP}.dim_activity_rule_info partition(dt='$do_date')
select
    ar.id,
    ar.activity_id,
    ai.activity_name,
    ar.activity_type,
    ai.start_time,
    ai.end_time,
    ai.create_time,
    ar.condition_amount,
    ar.condition_num,
    ar.benefit_amount,
    ar.benefit_discount,
    ar.benefit_level
from
(
    select
        id,
        activity_id,
        activity_type,
        condition_amount,
        condition_num,
        benefit_amount,
        benefit_discount,
        benefit_level
    from ${APP}.ods_activity_rule
    where dt='$do_date'
)ar
left join
(
    select
        id,
        activity_name,
        start_time,
        end_time,
        create_time
    from ${APP}.ods_activity_info
    where dt='$do_date'
)ai
on ar.activity_id=ai.id;
"

case $1 in
"dim_user_info"){
    hive -e "$dim_user_info"
};;
"dim_sku_info"){
    hive -e "$dim_sku_info"
};;
"dim_base_province"){
    hive -e "$dim_base_province"
};;
"dim_coupon_info"){
    hive -e "$dim_coupon_info"
};;
"dim_activity_rule_info"){
    hive -e "$dim_activity_rule_info"
};;
"all"){
    hive -e "$dim_user_info$dim_sku_info$dim_coupon_info$dim_activity_rule_info"
};;
esac

```





拉链表每日脚本

```sql
with
tmp as
(
    select
        old.id old_id,
        old.login_name old_login_name,
        old.nick_name old_nick_name,
        old.name old_name,
        old.phone_num old_phone_num,
        old.email old_email,
        old.user_level old_user_level,
        old.birthday old_birthday,
        old.gender old_gender,
        old.create_time old_create_time,
        old.operate_time old_operate_time,
        old.start_date old_start_date,
        old.end_date old_end_date,
        new.id new_id,
        new.login_name new_login_name,
        new.nick_name new_nick_name,
        new.name new_name,
        new.phone_num new_phone_num,
        new.email new_email,
        new.user_level new_user_level,
        new.birthday new_birthday,
        new.gender new_gender,
        new.create_time new_create_time,
        new.operate_time new_operate_time,
        new.start_date new_start_date,
        new.end_date new_end_date
    from
    (
        select
            id,
            login_name,
            nick_name,
            name,
            phone_num,
            email,
            user_level,
            birthday,
            gender,
            create_time,
            operate_time,
            start_date,
            end_date
        from dim_user_info
        where dt='9999-99-99'
    )old
    full outer join
    (
        select
            id,
            login_name,
            nick_name,
            md5(name) name,
            md5(phone_num) phone_num,
            md5(email) email,
            user_level,
            birthday,
            gender,
            create_time,
            operate_time,
            '2020-06-15' start_date,
            '9999-99-99' end_date
        from ods_user_info
        where dt='2020-06-15'
    )new
    on old.id=new.id
)
insert overwrite table dim_user_info partition(dt)
select
    nvl(new_id,old_id),
    nvl(new_login_name,old_login_name),
    nvl(new_nick_name,old_nick_name),
    nvl(new_name,old_name),
    nvl(new_phone_num,old_phone_num),
    nvl(new_email,old_email),
    nvl(new_user_level,old_user_level),
    nvl(new_birthday,old_birthday),
    nvl(new_gender,old_gender),
    nvl(new_create_time,old_create_time),
    nvl(new_operate_time,old_operate_time),
    nvl(new_start_date,old_start_date),
    nvl(new_end_date,old_end_date),
    nvl(new_end_date,old_end_date) dt
from tmp
union all
select
    old_id,
    old_login_name,
    old_nick_name,
    old_name,
    old_phone_num,
    old_email,
    old_user_level,
    old_birthday,
    old_gender,
    old_create_time,
    old_operate_time,
    old_start_date,
    cast(date_add('2020-06-15',-1) as string),
    cast(date_add('2020-06-15',-1) as string) dt
from tmp
where new_id is not null and old_id is not null;

```





## dwt层

### 用户主题

#### 每日装载

```sql
insert overwrite table dwt_user_topic partition(dt='2020-06-15')
select
    nvl(1d_ago.user_id,old.user_id),
    nvl(old.login_date_first,'2020-06-15'),
    if(1d_ago.user_id is not null,'2020-06-15',old.login_date_last),
    nvl(1d_ago.login_count,0),
    if(1d_ago.user_id is not null,1,0),
    nvl(old.login_last_7d_count,0)+nvl(1d_ago.login_count,0)- nvl(7d_ago.login_count,0),
    nvl(old.login_last_7d_day_count,0)+if(1d_ago.user_id is null,0,1)- if(7d_ago.user_id is null,0,1),
    nvl(old.login_last_30d_count,0)+nvl(1d_ago.login_count,0)- nvl(30d_ago.login_count,0),
    nvl(old.login_last_30d_day_count,0)+if(1d_ago.user_id is null,0,1)- if(30d_ago.user_id is null,0,1),
    nvl(old.login_count,0)+nvl(1d_ago.login_count,0),
    nvl(old.login_day_count,0)+if(1d_ago.user_id is not null,1,0),
    if(old.order_date_first is null and 1d_ago.order_count>0, '2020-06-15', old.order_date_first),
    if(1d_ago.order_count>0,'2020-06-15',old.order_date_last),
    nvl(1d_ago.order_count,0),
    nvl(1d_ago.order_activity_count,0),
    nvl(1d_ago.order_activity_reduce_amount,0.0),
    nvl(1d_ago.order_coupon_count,0),
    nvl(1d_ago.order_coupon_reduce_amount,0.0),
    nvl(1d_ago.order_original_amount,0.0),
    nvl(1d_ago.order_final_amount,0.0),
    nvl(old.order_last_7d_count,0)+nvl(1d_ago.order_count,0)- nvl(7d_ago.order_count,0),
    nvl(old.order_activity_last_7d_count,0)+nvl(1d_ago.order_activity_count,0)- nvl(7d_ago.order_activity_count,0),
    nvl(old.order_activity_reduce_last_7d_amount,0.0)+nvl(1d_ago.order_activity_reduce_amount,0.0)- nvl(7d_ago.order_activity_reduce_amount,0.0),
    nvl(old.order_coupon_last_7d_count,0)+nvl(1d_ago.order_coupon_count,0)- nvl(7d_ago.order_coupon_count,0),
    nvl(old.order_coupon_reduce_last_7d_amount,0.0)+nvl(1d_ago.order_coupon_reduce_amount,0.0)- nvl(7d_ago.order_coupon_reduce_amount,0.0),
    nvl(old.order_last_7d_original_amount,0.0)+nvl(1d_ago.order_original_amount,0.0)- nvl(7d_ago.order_original_amount,0.0),
    nvl(old.order_last_7d_final_amount,0.0)+nvl(1d_ago.order_final_amount,0.0)- nvl(7d_ago.order_final_amount,0.0),
    nvl(old.order_last_30d_count,0)+nvl(1d_ago.order_count,0)- nvl(30d_ago.order_count,0),
    nvl(old.order_activity_last_30d_count,0)+nvl(1d_ago.order_activity_count,0)- nvl(30d_ago.order_activity_count,0),
    nvl(old.order_activity_reduce_last_30d_amount,0.0)+nvl(1d_ago.order_activity_reduce_amount,0.0)- nvl(30d_ago.order_activity_reduce_amount,0.0),
    nvl(old.order_coupon_last_30d_count,0)+nvl(1d_ago.order_coupon_count,0)- nvl(30d_ago.order_coupon_count,0),
    nvl(old.order_coupon_reduce_last_30d_amount,0.0)+nvl(1d_ago.order_coupon_reduce_amount,0.0)- nvl(30d_ago.order_coupon_reduce_amount,0.0),
    nvl(old.order_last_30d_original_amount,0.0)+nvl(1d_ago.order_original_amount,0.0)- nvl(30d_ago.order_original_amount,0.0),
    nvl(old.order_last_30d_final_amount,0.0)+nvl(1d_ago.order_final_amount,0.0)- nvl(30d_ago.order_final_amount,0.0),
    nvl(old.order_count,0)+nvl(1d_ago.order_count,0),
    nvl(old.order_activity_count,0)+nvl(1d_ago.order_activity_count,0),
    nvl(old.order_activity_reduce_amount,0.0)+nvl(1d_ago.order_activity_reduce_amount,0.0),
    nvl(old.order_coupon_count,0)+nvl(1d_ago.order_coupon_count,0),
    nvl(old.order_coupon_reduce_amount,0.0)+nvl(1d_ago.order_coupon_reduce_amount,0.0),
    nvl(old.order_original_amount,0.0)+nvl(1d_ago.order_original_amount,0.0),
    nvl(old.order_final_amount,0.0)+nvl(1d_ago.order_final_amount,0.0),
    if(old.payment_date_first is null and 1d_ago.payment_count>0, '2020-06-15', old.payment_date_first),
    if(1d_ago.payment_count>0,'2020-06-15',old.payment_date_last),
    nvl(1d_ago.payment_count,0),
    nvl(1d_ago.payment_amount,0.0),
    nvl(old.payment_last_7d_count,0)+nvl(1d_ago.payment_count,0)-nvl(7d_ago.payment_count,0),
    nvl(old.payment_last_7d_amount,0.0)+nvl(1d_ago.payment_amount,0.0)-nvl(7d_ago.payment_amount,0.0),
    nvl(old.payment_last_30d_count,0)+nvl(1d_ago.payment_count,0)-nvl(30d_ago.payment_count,0),
    nvl(old.payment_last_30d_amount,0.0)+nvl(1d_ago.payment_amount,0.0)- nvl(30d_ago.payment_amount,0.0),
    nvl(old.payment_count,0)+nvl(1d_ago.payment_count,0),
    nvl(old.payment_amount,0.0)+nvl(1d_ago.payment_amount,0.0),
    nvl(1d_ago.refund_order_count,0),
    nvl(1d_ago.refund_order_num,0),
    nvl(1d_ago.refund_order_amount,0.0),
    nvl(old.refund_order_last_7d_count,0)+nvl(1d_ago.refund_order_count,0)- nvl(7d_ago.refund_order_count,0),
    nvl(old.refund_order_last_7d_num,0)+nvl(1d_ago.refund_order_num, 0)- nvl(7d_ago.refund_order_num,0),
    nvl(old.refund_order_last_7d_amount,0.0)+ nvl(1d_ago.refund_order_amount,0.0)- nvl(7d_ago.refund_order_amount,0.0),
    nvl(old.refund_order_last_30d_count,0)+nvl(1d_ago.refund_order_count,0)- nvl(30d_ago.refund_order_count,0),
    nvl(old.refund_order_last_30d_num,0)+nvl(1d_ago.refund_order_num, 0)- nvl(30d_ago.refund_order_num,0),
    nvl(old.refund_order_last_30d_amount,0.0)+ nvl(1d_ago.refund_order_amount,0.0)- nvl(30d_ago.refund_order_amount,0.0),
    nvl(old.refund_order_count,0)+nvl(1d_ago.refund_order_count,0),
    nvl(old.refund_order_num,0)+nvl(1d_ago.refund_order_num,0),
    nvl(old.refund_order_amount,0.0)+ nvl(1d_ago.refund_order_amount,0.0),
    nvl(1d_ago.refund_payment_count,0),
    nvl(1d_ago.refund_payment_num,0),
    nvl(1d_ago.refund_payment_amount,0.0),
    nvl(old.refund_payment_last_7d_count,0)+nvl(1d_ago.refund_payment_count,0)-nvl(7d_ago.refund_payment_count,0),
    nvl(old.refund_payment_last_7d_num,0)+nvl(1d_ago.refund_payment_num,0)- nvl(7d_ago.refund_payment_num,0),
    nvl(old.refund_payment_last_7d_amount,0.0)+ nvl(1d_ago.refund_payment_amount,0.0)- nvl(7d_ago.refund_payment_amount,0.0),
    nvl(old.refund_payment_last_30d_count,0)+nvl(1d_ago.refund_payment_count,0)-nvl(30d_ago.refund_payment_count,0),
    nvl(old.refund_payment_last_30d_num,0)+nvl(1d_ago.refund_payment_num,0)- nvl(30d_ago.refund_payment_num,0),
    nvl(old.refund_payment_last_30d_amount,0.0)+ nvl(1d_ago.refund_payment_amount,0.0)- nvl(30d_ago.refund_payment_amount,0.0),
    nvl(old.refund_payment_count,0)+nvl(1d_ago.refund_payment_count,0),
    nvl(old.refund_payment_num,0)+nvl(1d_ago.refund_payment_num,0),
    nvl(old.refund_payment_amount,0.0)+nvl(1d_ago.refund_payment_amount,0.0),
    nvl(1d_ago.cart_count,0),
    nvl(old.cart_last_7d_count,0)+nvl(1d_ago.cart_count,0)-nvl(7d_ago.cart_count,0),
    nvl(old.cart_last_30d_count,0)+nvl(1d_ago.cart_count,0)-nvl(30d_ago.cart_count,0),
    nvl(old.cart_count,0)+nvl(1d_ago.cart_count,0),
    nvl(1d_ago.favor_count,0),
    nvl(old.favor_last_7d_count,0)+nvl(1d_ago.favor_count,0)- nvl(7d_ago.favor_count,0),
    nvl(old.favor_last_30d_count,0)+nvl(1d_ago.favor_count,0)- nvl(30d_ago.favor_count,0),
    nvl(old.favor_count,0)+nvl(1d_ago.favor_count,0),
    nvl(1d_ago.coupon_get_count,0),
    nvl(1d_ago.coupon_using_count,0),
    nvl(1d_ago.coupon_used_count,0),
    nvl(old.coupon_last_7d_get_count,0)+nvl(1d_ago.coupon_get_count,0)- nvl(7d_ago.coupon_get_count,0),
    nvl(old.coupon_last_7d_using_count,0)+nvl(1d_ago.coupon_using_count,0)- nvl(7d_ago.coupon_using_count,0),
    nvl(old.coupon_last_7d_used_count,0)+ nvl(1d_ago.coupon_used_count,0)- nvl(7d_ago.coupon_used_count,0),
    nvl(old.coupon_last_30d_get_count,0)+nvl(1d_ago.coupon_get_count,0)- nvl(30d_ago.coupon_get_count,0),
    nvl(old.coupon_last_30d_using_count,0)+nvl(1d_ago.coupon_using_count,0)- nvl(30d_ago.coupon_using_count,0),
    nvl(old.coupon_last_30d_used_count,0)+ nvl(1d_ago.coupon_used_count,0)- nvl(30d_ago.coupon_used_count,0),
    nvl(old.coupon_get_count,0)+nvl(1d_ago.coupon_get_count,0),
    nvl(old.coupon_using_count,0)+nvl(1d_ago.coupon_using_count,0),
    nvl(old.coupon_used_count,0)+nvl(1d_ago.coupon_used_count,0),
    nvl(1d_ago.appraise_good_count,0),
    nvl(1d_ago.appraise_mid_count,0),
    nvl(1d_ago.appraise_bad_count,0),
    nvl(1d_ago.appraise_default_count,0),
    nvl(old.appraise_last_7d_good_count,0)+nvl(1d_ago.appraise_good_count,0)- nvl(7d_ago.appraise_good_count,0),
    nvl(old.appraise_last_7d_mid_count,0)+nvl(1d_ago.appraise_mid_count,0)-nvl(7d_ago.appraise_mid_count,0),
    nvl(old.appraise_last_7d_bad_count,0)+nvl(1d_ago.appraise_bad_count,0)-nvl(7d_ago.appraise_bad_count,0),
    nvl(old.appraise_last_7d_default_count,0)+nvl(1d_ago.appraise_default_count,0)-nvl(7d_ago.appraise_default_count,0),
    nvl(old.appraise_last_30d_good_count,0)+nvl(1d_ago.appraise_good_count,0)- nvl(30d_ago.appraise_good_count,0),
    nvl(old.appraise_last_30d_mid_count,0)+nvl(1d_ago.appraise_mid_count,0)-nvl(30d_ago.appraise_mid_count,0),
    nvl(old.appraise_last_30d_bad_count,0)+nvl(1d_ago.appraise_bad_count,0)-nvl(30d_ago.appraise_bad_count,0),
    nvl(old.appraise_last_30d_default_count,0)+nvl(1d_ago.appraise_default_count,0)-nvl(30d_ago.appraise_default_count,0),
    nvl(old.appraise_good_count,0)+nvl(1d_ago.appraise_good_count,0),
    nvl(old.appraise_mid_count,0)+nvl(1d_ago.appraise_mid_count, 0),
    nvl(old.appraise_bad_count,0)+nvl(1d_ago.appraise_bad_count,0),
    nvl(old.appraise_default_count,0)+nvl(1d_ago.appraise_default_count,0)
from
(
    select
        user_id,
        login_date_first,
        login_date_last,
        login_date_1d_count,
        login_last_1d_day_count,
        login_last_7d_count,
        login_last_7d_day_count,
        login_last_30d_count,
        login_last_30d_day_count,
        login_count,
        login_day_count,
        order_date_first,
        order_date_last,
        order_last_1d_count,
        order_activity_last_1d_count,
        order_activity_reduce_last_1d_amount,
        order_coupon_last_1d_count,
        order_coupon_reduce_last_1d_amount,
        order_last_1d_original_amount,
        order_last_1d_final_amount,
        order_last_7d_count,
        order_activity_last_7d_count,
        order_activity_reduce_last_7d_amount,
        order_coupon_last_7d_count,
        order_coupon_reduce_last_7d_amount,
        order_last_7d_original_amount,
        order_last_7d_final_amount,
        order_last_30d_count,
        order_activity_last_30d_count,
        order_activity_reduce_last_30d_amount,
        order_coupon_last_30d_count,
        order_coupon_reduce_last_30d_amount,
        order_last_30d_original_amount,
        order_last_30d_final_amount,
        order_count,
        order_activity_count,
        order_activity_reduce_amount,
        order_coupon_count,
        order_coupon_reduce_amount,
        order_original_amount,
        order_final_amount,
        payment_date_first,
        payment_date_last,
        payment_last_1d_count,
        payment_last_1d_amount,
        payment_last_7d_count,
        payment_last_7d_amount,
        payment_last_30d_count,
        payment_last_30d_amount,
        payment_count,
        payment_amount,
        refund_order_last_1d_count,
        refund_order_last_1d_num,
        refund_order_last_1d_amount,
        refund_order_last_7d_count,
        refund_order_last_7d_num,
        refund_order_last_7d_amount,
        refund_order_last_30d_count,
        refund_order_last_30d_num,
        refund_order_last_30d_amount,
        refund_order_count,
        refund_order_num,
        refund_order_amount,
        refund_payment_last_1d_count,
        refund_payment_last_1d_num,
        refund_payment_last_1d_amount,
        refund_payment_last_7d_count,
        refund_payment_last_7d_num,
        refund_payment_last_7d_amount,
        refund_payment_last_30d_count,
        refund_payment_last_30d_num,
        refund_payment_last_30d_amount,
        refund_payment_count,
        refund_payment_num,
        refund_payment_amount,
        cart_last_1d_count,
        cart_last_7d_count,
        cart_last_30d_count,
        cart_count,
        favor_last_1d_count,
        favor_last_7d_count,
        favor_last_30d_count,
        favor_count,
        coupon_last_1d_get_count,
        coupon_last_1d_using_count,
        coupon_last_1d_used_count,
        coupon_last_7d_get_count,
        coupon_last_7d_using_count,
        coupon_last_7d_used_count,
        coupon_last_30d_get_count,
        coupon_last_30d_using_count,
        coupon_last_30d_used_count,
        coupon_get_count,
        coupon_using_count,
        coupon_used_count,
        appraise_last_1d_good_count,
        appraise_last_1d_mid_count,
        appraise_last_1d_bad_count,
        appraise_last_1d_default_count,
        appraise_last_7d_good_count,
        appraise_last_7d_mid_count,
        appraise_last_7d_bad_count,
        appraise_last_7d_default_count,
        appraise_last_30d_good_count,
        appraise_last_30d_mid_count,
        appraise_last_30d_bad_count,
        appraise_last_30d_default_count,
        appraise_good_count,
        appraise_mid_count,
        appraise_bad_count,
        appraise_default_count
    from dwt_user_topic
    where dt=date_add('2020-06-15',-1)
)old
full outer join
(
    select
        user_id,
        login_count,
        cart_count,
        favor_count,
        order_count,
        order_activity_count,
        order_activity_reduce_amount,
        order_coupon_count,
        order_coupon_reduce_amount,
        order_original_amount,
        order_final_amount,
        payment_count,
        payment_amount,
        refund_order_count,
        refund_order_num,
        refund_order_amount,
        refund_payment_count,
        refund_payment_num,
        refund_payment_amount,
        coupon_get_count,
        coupon_using_count,
        coupon_used_count,
        appraise_good_count,
        appraise_mid_count,
        appraise_bad_count,
        appraise_default_count
    from dws_user_action_daycount
    where dt='2020-06-15'
)1d_ago
on old.user_id=1d_ago.user_id
left join
(
    select
        user_id,
        login_count,
        cart_count,
        favor_count,
        order_count,
        order_activity_count,
        order_activity_reduce_amount,
        order_coupon_count,
        order_coupon_reduce_amount,
        order_original_amount,
        order_final_amount,
        payment_count,
        payment_amount,
        refund_order_count,
        refund_order_num,
        refund_order_amount,
        refund_payment_count,
        refund_payment_num,
        refund_payment_amount,
        coupon_get_count,
        coupon_using_count,
        coupon_used_count,
        appraise_good_count,
        appraise_mid_count,
        appraise_bad_count,
        appraise_default_count
    from dws_user_action_daycount
    where dt=date_add('2020-06-15',-7)
)7d_ago
on old.user_id=7d_ago.user_id
left join
(
    select
        user_id,
        login_count,
        cart_count,
        favor_count,
        order_count,
        order_activity_count,
        order_activity_reduce_amount,
        order_coupon_count,
        order_coupon_reduce_amount,
        order_original_amount,
        order_final_amount,
        payment_count,
        payment_amount,
        refund_order_count,
        refund_order_num,
        refund_order_amount,
        refund_payment_count,
        refund_payment_num,
        refund_payment_amount,
        coupon_get_count,
        coupon_using_count,
        coupon_used_count,
        appraise_good_count,
        appraise_mid_count,
        appraise_bad_count,
        appraise_default_count
    from dws_user_action_daycount
    where dt=date_add('2020-06-15',-30)
)30d_ago
on old.user_id=30d_ago.user_id;

```





### dwt层每日装载

```sql
#!/bin/bash

APP=gmall
# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$2" ] ;then
    do_date=$2
else 
    do_date=`date -d "-1 day" +%F`
fi

clear_date=`date -d "$do_date -2 day" +%F`

dwt_visitor_topic="
insert overwrite table ${APP}.dwt_visitor_topic partition(dt='$do_date')
select
    nvl(1d_ago.mid_id,old.mid_id),
    nvl(1d_ago.brand,old.brand),
    nvl(1d_ago.model,old.model),
    nvl(1d_ago.channel,old.channel),
    nvl(1d_ago.os,old.os),
    nvl(1d_ago.area_code,old.area_code),
    nvl(1d_ago.version_code,old.version_code),
    case when old.mid_id is null and 1d_ago.is_new=1 then '$do_date'
         when old.mid_id is null and 1d_ago.is_new=0 then '2020-06-13'--无法获取准确的首次登录日期，给定一个数仓搭建日之前的日期
         else old.visit_date_first end,
    if(1d_ago.mid_id is not null,'$do_date',old.visit_date_last),
    nvl(1d_ago.visit_count,0),
    if(1d_ago.mid_id is null,0,1),
    nvl(old.visit_last_7d_count,0)+nvl(1d_ago.visit_count,0)- nvl(7d_ago.visit_count,0),
    nvl(old.visit_last_7d_day_count,0)+if(1d_ago.mid_id is null,0,1)- if(7d_ago.mid_id is null,0,1),
    nvl(old.visit_last_30d_count,0)+nvl(1d_ago.visit_count,0)- nvl(30d_ago.visit_count,0),
    nvl(old.visit_last_30d_day_count,0)+if(1d_ago.mid_id is null,0,1)- if(30d_ago.mid_id is null,0,1),
    nvl(old.visit_count,0)+nvl(1d_ago.visit_count,0),
    nvl(old.visit_day_count,0)+if(1d_ago.mid_id is null,0,1)
from
(
    select
        mid_id,
        brand,
        model,
        channel,
        os,
        area_code,
        version_code,
        visit_date_first,
        visit_date_last,
        visit_last_1d_count,
        visit_last_1d_day_count,
        visit_last_7d_count,
        visit_last_7d_day_count,
        visit_last_30d_count,
        visit_last_30d_day_count,
        visit_count,
        visit_day_count
    from ${APP}.dwt_visitor_topic
    where dt=date_add('$do_date',-1)
)old
full outer join
(
    select
        mid_id,
        brand,
        model,
        is_new,
        channel,
        os,
        area_code,
        version_code,
        visit_count
    from ${APP}.dws_visitor_action_daycount
    where dt='$do_date'
)1d_ago
on old.mid_id=1d_ago.mid_id
left join
(
    select
        mid_id,
        brand,
        model,
        is_new,
        channel,
        os,
        area_code,
        version_code,
        visit_count
    from ${APP}.dws_visitor_action_daycount
    where dt=date_add('$do_date',-7)
)7d_ago
on old.mid_id=7d_ago.mid_id
left join
(
    select
        mid_id,
        brand,
        model,
        is_new,
        channel,
        os,
        area_code,
        version_code,
        visit_count
    from ${APP}.dws_visitor_action_daycount
    where dt=date_add('$do_date',-30)
)30d_ago
on old.mid_id=30d_ago.mid_id;
alter table ${APP}.dwt_visitor_topic drop partition(dt='$clear_date');
"

dwt_user_topic="
insert overwrite table ${APP}.dwt_user_topic partition(dt='$do_date')
select
    nvl(1d_ago.user_id,old.user_id),
    nvl(old.login_date_first,'$do_date'),
    if(1d_ago.user_id is not null,'$do_date',old.login_date_last),
    nvl(1d_ago.login_count,0),
    if(1d_ago.user_id is not null,1,0),
    nvl(old.login_last_7d_count,0)+nvl(1d_ago.login_count,0)- nvl(7d_ago.login_count,0),
    nvl(old.login_last_7d_day_count,0)+if(1d_ago.user_id is null,0,1)- if(7d_ago.user_id is null,0,1),
    nvl(old.login_last_30d_count,0)+nvl(1d_ago.login_count,0)- nvl(30d_ago.login_count,0),
    nvl(old.login_last_30d_day_count,0)+if(1d_ago.user_id is null,0,1)- if(30d_ago.user_id is null,0,1),
    nvl(old.login_count,0)+nvl(1d_ago.login_count,0),
    nvl(old.login_day_count,0)+if(1d_ago.user_id is not null,1,0),
    if(old.order_date_first is null and 1d_ago.order_count>0, '$do_date', old.order_date_first),
    if(1d_ago.order_count>0,'$do_date',old.order_date_last),
    nvl(1d_ago.order_count,0),
    nvl(1d_ago.order_activity_count,0),
    nvl(1d_ago.order_activity_reduce_amount,0.0),
    nvl(1d_ago.order_coupon_count,0),
    nvl(1d_ago.order_coupon_reduce_amount,0.0),
    nvl(1d_ago.order_original_amount,0.0),
    nvl(1d_ago.order_final_amount,0.0),
    nvl(old.order_last_7d_count,0)+nvl(1d_ago.order_count,0)- nvl(7d_ago.order_count,0),
    nvl(old.order_activity_last_7d_count,0)+nvl(1d_ago.order_activity_count,0)- nvl(7d_ago.order_activity_count,0),
    nvl(old.order_activity_reduce_last_7d_amount,0.0)+nvl(1d_ago.order_activity_reduce_amount,0.0)- nvl(7d_ago.order_activity_reduce_amount,0.0),
    nvl(old.order_coupon_last_7d_count,0)+nvl(1d_ago.order_coupon_count,0)- nvl(7d_ago.order_coupon_count,0),
    nvl(old.order_coupon_reduce_last_7d_amount,0.0)+nvl(1d_ago.order_coupon_reduce_amount,0.0)- nvl(7d_ago.order_coupon_reduce_amount,0.0),
    nvl(old.order_last_7d_original_amount,0.0)+nvl(1d_ago.order_original_amount,0.0)- nvl(7d_ago.order_original_amount,0.0),
    nvl(old.order_last_7d_final_amount,0.0)+nvl(1d_ago.order_final_amount,0.0)- nvl(7d_ago.order_final_amount,0.0),
    nvl(old.order_last_30d_count,0)+nvl(1d_ago.order_count,0)- nvl(30d_ago.order_count,0),
    nvl(old.order_activity_last_30d_count,0)+nvl(1d_ago.order_activity_count,0)- nvl(30d_ago.order_activity_count,0),
    nvl(old.order_activity_reduce_last_30d_amount,0.0)+nvl(1d_ago.order_activity_reduce_amount,0.0)- nvl(30d_ago.order_activity_reduce_amount,0.0),
    nvl(old.order_coupon_last_30d_count,0)+nvl(1d_ago.order_coupon_count,0)- nvl(30d_ago.order_coupon_count,0),
    nvl(old.order_coupon_reduce_last_30d_amount,0.0)+nvl(1d_ago.order_coupon_reduce_amount,0.0)- nvl(30d_ago.order_coupon_reduce_amount,0.0),
    nvl(old.order_last_30d_original_amount,0.0)+nvl(1d_ago.order_original_amount,0.0)- nvl(30d_ago.order_original_amount,0.0),
    nvl(old.order_last_30d_final_amount,0.0)+nvl(1d_ago.order_final_amount,0.0)- nvl(30d_ago.order_final_amount,0.0),
    nvl(old.order_count,0)+nvl(1d_ago.order_count,0),
    nvl(old.order_activity_count,0)+nvl(1d_ago.order_activity_count,0),
    nvl(old.order_activity_reduce_amount,0.0)+nvl(1d_ago.order_activity_reduce_amount,0.0),
    nvl(old.order_coupon_count,0)+nvl(1d_ago.order_coupon_count,0),
    nvl(old.order_coupon_reduce_amount,0.0)+nvl(1d_ago.order_coupon_reduce_amount,0.0),
    nvl(old.order_original_amount,0.0)+nvl(1d_ago.order_original_amount,0.0),
    nvl(old.order_final_amount,0.0)+nvl(1d_ago.order_final_amount,0.0),
    if(old.payment_date_first is null and 1d_ago.payment_count>0, '$do_date', old.payment_date_first),
    if(1d_ago.payment_count>0,'$do_date',old.payment_date_last),
    nvl(1d_ago.payment_count,0),
    nvl(1d_ago.payment_amount,0.0),
    nvl(old.payment_last_7d_count,0)+nvl(1d_ago.payment_count,0)-nvl(7d_ago.payment_count,0),
    nvl(old.payment_last_7d_amount,0.0)+nvl(1d_ago.payment_amount,0.0)-nvl(7d_ago.payment_amount,0.0),
    nvl(old.payment_last_30d_count,0)+nvl(1d_ago.payment_count,0)-nvl(30d_ago.payment_count,0),
    nvl(old.payment_last_30d_amount,0.0)+nvl(1d_ago.payment_amount,0.0)- nvl(30d_ago.payment_amount,0.0),
    nvl(old.payment_count,0)+nvl(1d_ago.payment_count,0),
    nvl(old.payment_amount,0.0)+nvl(1d_ago.payment_amount,0.0),
    nvl(1d_ago.refund_order_count,0),
    nvl(1d_ago.refund_order_num,0),
    nvl(1d_ago.refund_order_amount,0.0),
    nvl(old.refund_order_last_7d_count,0)+nvl(1d_ago.refund_order_count,0)- nvl(7d_ago.refund_order_count,0),
    nvl(old.refund_order_last_7d_num,0)+nvl(1d_ago.refund_order_num, 0)- nvl(7d_ago.refund_order_num,0),
    nvl(old.refund_order_last_7d_amount,0.0)+ nvl(1d_ago.refund_order_amount,0.0)- nvl(7d_ago.refund_order_amount,0.0),
    nvl(old.refund_order_last_30d_count,0)+nvl(1d_ago.refund_order_count,0)- nvl(30d_ago.refund_order_count,0),
    nvl(old.refund_order_last_30d_num,0)+nvl(1d_ago.refund_order_num, 0)- nvl(30d_ago.refund_order_num,0),
    nvl(old.refund_order_last_30d_amount,0.0)+ nvl(1d_ago.refund_order_amount,0.0)- nvl(30d_ago.refund_order_amount,0.0),
    nvl(old.refund_order_count,0)+nvl(1d_ago.refund_order_count,0),
    nvl(old.refund_order_num,0)+nvl(1d_ago.refund_order_num,0),
    nvl(old.refund_order_amount,0.0)+ nvl(1d_ago.refund_order_amount,0.0),
    nvl(1d_ago.refund_payment_count,0),
    nvl(1d_ago.refund_payment_num,0),
    nvl(1d_ago.refund_payment_amount,0.0),
    nvl(old.refund_payment_last_7d_count,0)+nvl(1d_ago.refund_payment_count,0)-nvl(7d_ago.refund_payment_count,0),
    nvl(old.refund_payment_last_7d_num,0)+nvl(1d_ago.refund_payment_num,0)- nvl(7d_ago.refund_payment_num,0),
    nvl(old.refund_payment_last_7d_amount,0.0)+ nvl(1d_ago.refund_payment_amount,0.0)- nvl(7d_ago.refund_payment_amount,0.0),
    nvl(old.refund_payment_last_30d_count,0)+nvl(1d_ago.refund_payment_count,0)-nvl(30d_ago.refund_payment_count,0),
    nvl(old.refund_payment_last_30d_num,0)+nvl(1d_ago.refund_payment_num,0)- nvl(30d_ago.refund_payment_num,0),
    nvl(old.refund_payment_last_30d_amount,0.0)+ nvl(1d_ago.refund_payment_amount,0.0)- nvl(30d_ago.refund_payment_amount,0.0),
    nvl(old.refund_payment_count,0)+nvl(1d_ago.refund_payment_count,0),
    nvl(old.refund_payment_num,0)+nvl(1d_ago.refund_payment_num,0),
    nvl(old.refund_payment_amount,0.0)+nvl(1d_ago.refund_payment_amount,0.0),
    nvl(1d_ago.cart_count,0),
    nvl(old.cart_last_7d_count,0)+nvl(1d_ago.cart_count,0)-nvl(7d_ago.cart_count,0),
    nvl(old.cart_last_30d_count,0)+nvl(1d_ago.cart_count,0)-nvl(30d_ago.cart_count,0),
    nvl(old.cart_count,0)+nvl(1d_ago.cart_count,0),
    nvl(1d_ago.favor_count,0),
    nvl(old.favor_last_7d_count,0)+nvl(1d_ago.favor_count,0)- nvl(7d_ago.favor_count,0),
    nvl(old.favor_last_30d_count,0)+nvl(1d_ago.favor_count,0)- nvl(30d_ago.favor_count,0),
    nvl(old.favor_count,0)+nvl(1d_ago.favor_count,0),
    nvl(1d_ago.coupon_get_count,0),
    nvl(1d_ago.coupon_using_count,0),
    nvl(1d_ago.coupon_used_count,0),
    nvl(old.coupon_last_7d_get_count,0)+nvl(1d_ago.coupon_get_count,0)- nvl(7d_ago.coupon_get_count,0),
    nvl(old.coupon_last_7d_using_count,0)+nvl(1d_ago.coupon_using_count,0)- nvl(7d_ago.coupon_using_count,0),
    nvl(old.coupon_last_7d_used_count,0)+ nvl(1d_ago.coupon_used_count,0)- nvl(7d_ago.coupon_used_count,0),
    nvl(old.coupon_last_30d_get_count,0)+nvl(1d_ago.coupon_get_count,0)- nvl(30d_ago.coupon_get_count,0),
    nvl(old.coupon_last_30d_using_count,0)+nvl(1d_ago.coupon_using_count,0)- nvl(30d_ago.coupon_using_count,0),
    nvl(old.coupon_last_30d_used_count,0)+ nvl(1d_ago.coupon_used_count,0)- nvl(30d_ago.coupon_used_count,0),
    nvl(old.coupon_get_count,0)+nvl(1d_ago.coupon_get_count,0),
    nvl(old.coupon_using_count,0)+nvl(1d_ago.coupon_using_count,0),
    nvl(old.coupon_used_count,0)+nvl(1d_ago.coupon_used_count,0),
    nvl(1d_ago.appraise_good_count,0),
    nvl(1d_ago.appraise_mid_count,0),
    nvl(1d_ago.appraise_bad_count,0),
    nvl(old.appraise_last_7d_default_count,0)+nvl(1d_ago.appraise_default_count,0)-nvl(7d_ago.appraise_default_count,0),
    nvl(old.appraise_last_7d_good_count,0)+nvl(1d_ago.appraise_good_count,0)- nvl(7d_ago.appraise_good_count,0),
    nvl(old.appraise_last_7d_mid_count,0)+nvl(1d_ago.appraise_mid_count,0)-nvl(7d_ago.appraise_mid_count,0),
    nvl(old.appraise_last_7d_bad_count,0)+nvl(1d_ago.appraise_bad_count,0)-nvl(7d_ago.appraise_bad_count,0),
    nvl(old.appraise_last_7d_default_count,0)+nvl(1d_ago.appraise_default_count,0)-nvl(7d_ago.appraise_default_count,0),
    nvl(old.appraise_last_30d_good_count,0)+nvl(1d_ago.appraise_good_count,0)- nvl(30d_ago.appraise_good_count,0),
    nvl(old.appraise_last_30d_mid_count,0)+nvl(1d_ago.appraise_mid_count,0)-nvl(30d_ago.appraise_mid_count,0),
    nvl(old.appraise_last_30d_bad_count,0)+nvl(1d_ago.appraise_bad_count,0)-nvl(30d_ago.appraise_bad_count,0),
    nvl(old.appraise_last_30d_default_count,0)+nvl(1d_ago.appraise_default_count,0)-nvl(30d_ago.appraise_default_count,0),
    nvl(old.appraise_good_count,0)+nvl(1d_ago.appraise_good_count,0),
    nvl(old.appraise_mid_count,0)+nvl(1d_ago.appraise_mid_count, 0),
    nvl(old.appraise_bad_count,0)+nvl(1d_ago.appraise_bad_count,0),
    nvl(old.appraise_default_count,0)+nvl(1d_ago.appraise_default_count,0)
from
(
    select
        user_id,
        login_date_first,
        login_date_last,
        login_date_1d_count,
        login_last_1d_day_count,
        login_last_7d_count,
        login_last_7d_day_count,
        login_last_30d_count,
        login_last_30d_day_count,
        login_count,
        login_day_count,
        order_date_first,
        order_date_last,
        order_last_1d_count,
        order_activity_last_1d_count,
        order_activity_reduce_last_1d_amount,
        order_coupon_last_1d_count,
        order_coupon_reduce_last_1d_amount,
        order_last_1d_original_amount,
        order_last_1d_final_amount,
        order_last_7d_count,
        order_activity_last_7d_count,
        order_activity_reduce_last_7d_amount,
        order_coupon_last_7d_count,
        order_coupon_reduce_last_7d_amount,
        order_last_7d_original_amount,
        order_last_7d_final_amount,
        order_last_30d_count,
        order_activity_last_30d_count,
        order_activity_reduce_last_30d_amount,
        order_coupon_last_30d_count,
        order_coupon_reduce_last_30d_amount,
        order_last_30d_original_amount,
        order_last_30d_final_amount,
        order_count,
        order_activity_count,
        order_activity_reduce_amount,
        order_coupon_count,
        order_coupon_reduce_amount,
        order_original_amount,
        order_final_amount,
        payment_date_first,
        payment_date_last,
        payment_last_1d_count,
        payment_last_1d_amount,
        payment_last_7d_count,
        payment_last_7d_amount,
        payment_last_30d_count,
        payment_last_30d_amount,
        payment_count,
        payment_amount,
        refund_order_last_1d_count,
        refund_order_last_1d_num,
        refund_order_last_1d_amount,
        refund_order_last_7d_count,
        refund_order_last_7d_num,
        refund_order_last_7d_amount,
        refund_order_last_30d_count,
        refund_order_last_30d_num,
        refund_order_last_30d_amount,
        refund_order_count,
        refund_order_num,
        refund_order_amount,
        refund_payment_last_1d_count,
        refund_payment_last_1d_num,
        refund_payment_last_1d_amount,
        refund_payment_last_7d_count,
        refund_payment_last_7d_num,
        refund_payment_last_7d_amount,
        refund_payment_last_30d_count,
        refund_payment_last_30d_num,
        refund_payment_last_30d_amount,
        refund_payment_count,
        refund_payment_num,
        refund_payment_amount,
        cart_last_1d_count,
        cart_last_7d_count,
        cart_last_30d_count,
        cart_count,
        favor_last_1d_count,
        favor_last_7d_count,
        favor_last_30d_count,
        favor_count,
        coupon_last_1d_get_count,
        coupon_last_1d_using_count,
        coupon_last_1d_used_count,
        coupon_last_7d_get_count,
        coupon_last_7d_using_count,
        coupon_last_7d_used_count,
        coupon_last_30d_get_count,
        coupon_last_30d_using_count,
        coupon_last_30d_used_count,
        coupon_get_count,
        coupon_using_count,
        coupon_used_count,
        appraise_last_1d_good_count,
        appraise_last_1d_mid_count,
        appraise_last_1d_bad_count,
        appraise_last_1d_default_count,
        appraise_last_7d_good_count,
        appraise_last_7d_mid_count,
        appraise_last_7d_bad_count,
        appraise_last_7d_default_count,
        appraise_last_30d_good_count,
        appraise_last_30d_mid_count,
        appraise_last_30d_bad_count,
        appraise_last_30d_default_count,
        appraise_good_count,
        appraise_mid_count,
        appraise_bad_count,
        appraise_default_count
    from ${APP}.dwt_user_topic
    where dt=date_add('$do_date',-1)
)old
full outer join
(
    select
        user_id,
        login_count,
        cart_count,
        favor_count,
        order_count,
        order_activity_count,
        order_activity_reduce_amount,
        order_coupon_count,
        order_coupon_reduce_amount,
        order_original_amount,
        order_final_amount,
        payment_count,
        payment_amount,
        refund_order_count,
        refund_order_num,
        refund_order_amount,
        refund_payment_count,
        refund_payment_num,
        refund_payment_amount,
        coupon_get_count,
        coupon_using_count,
        coupon_used_count,
        appraise_good_count,
        appraise_mid_count,
        appraise_bad_count,
        appraise_default_count
    from ${APP}.dws_user_action_daycount
    where dt='$do_date'
)1d_ago
on old.user_id=1d_ago.user_id
left join
(
    select
        user_id,
        login_count,
        cart_count,
        favor_count,
        order_count,
        order_activity_count,
        order_activity_reduce_amount,
        order_coupon_count,
        order_coupon_reduce_amount,
        order_original_amount,
        order_final_amount,
        payment_count,
        payment_amount,
        refund_order_count,
        refund_order_num,
        refund_order_amount,
        refund_payment_count,
        refund_payment_num,
        refund_payment_amount,
        coupon_get_count,
        coupon_using_count,
        coupon_used_count,
        appraise_good_count,
        appraise_mid_count,
        appraise_bad_count,
        appraise_default_count
    from ${APP}.dws_user_action_daycount
    where dt=date_add('$do_date',-7)
)7d_ago
on old.user_id=7d_ago.user_id
left join
(
    select
        user_id,
        login_count,
        cart_count,
        favor_count,
        order_count,
        order_activity_count,
        order_activity_reduce_amount,
        order_coupon_count,
        order_coupon_reduce_amount,
        order_original_amount,
        order_final_amount,
        payment_count,
        payment_amount,
        refund_order_count,
        refund_order_num,
        refund_order_amount,
        refund_payment_count,
        refund_payment_num,
        refund_payment_amount,
        coupon_get_count,
        coupon_using_count,
        coupon_used_count,
        appraise_good_count,
        appraise_mid_count,
        appraise_bad_count,
        appraise_default_count
    from ${APP}.dws_user_action_daycount
    where dt=date_add('$do_date',-30)
)30d_ago
on old.user_id=30d_ago.user_id;
alter table ${APP}.dwt_user_topic drop partition(dt='$clear_date');
"

dwt_sku_topic="
insert overwrite table ${APP}.dwt_sku_topic partition(dt='$do_date')
select
    nvl(1d_ago.sku_id,old.sku_id),
    nvl(1d_ago.order_count,0),
    nvl(1d_ago.order_num,0),
    nvl(1d_ago.order_activity_count,0),
    nvl(1d_ago.order_coupon_count,0),
    nvl(1d_ago.order_activity_reduce_amount,0.0),
    nvl(1d_ago.order_coupon_reduce_amount,0.0),
    nvl(1d_ago.order_original_amount,0.0),
    nvl(1d_ago.order_final_amount,0.0),
    nvl(old.order_last_7d_count,0)+nvl(1d_ago.order_count,0)- nvl(7d_ago.order_count,0),
    nvl(old.order_last_7d_num,0)+nvl(1d_ago.order_num,0)- nvl(7d_ago.order_num,0),
    nvl(old.order_activity_last_7d_count,0)+nvl(1d_ago.order_activity_count,0)- nvl(7d_ago.order_activity_count,0),
    nvl(old.order_coupon_last_7d_count,0)+nvl(1d_ago.order_coupon_count,0)- nvl(7d_ago.order_coupon_count,0),
    nvl(old.order_activity_reduce_last_7d_amount,0.0)+nvl(1d_ago.order_activity_reduce_amount,0.0)- nvl(7d_ago.order_activity_reduce_amount,0.0),
    nvl(old.order_coupon_reduce_last_7d_amount,0.0)+nvl(1d_ago.order_coupon_reduce_amount,0.0)- nvl(7d_ago.order_coupon_reduce_amount,0.0),
    nvl(old.order_last_7d_original_amount,0.0)+nvl(1d_ago.order_original_amount,0.0)- nvl(7d_ago.order_original_amount,0.0),
    nvl(old.order_last_7d_final_amount,0.0)+nvl(1d_ago.order_final_amount,0.0)- nvl(7d_ago.order_final_amount,0.0),
    nvl(old.order_last_30d_count,0)+nvl(1d_ago.order_count,0)- nvl(30d_ago.order_count,0),
    nvl(old.order_last_30d_num,0)+nvl(1d_ago.order_num,0)- nvl(30d_ago.order_num,0),
    nvl(old.order_activity_last_30d_count,0)+nvl(1d_ago.order_activity_count,0)- nvl(30d_ago.order_activity_count,0),
    nvl(old.order_coupon_last_30d_count,0)+nvl(1d_ago.order_coupon_count,0)- nvl(30d_ago.order_coupon_count,0),
    nvl(old.order_activity_reduce_last_30d_amount,0.0)+nvl(1d_ago.order_activity_reduce_amount,0.0)- nvl(30d_ago.order_activity_reduce_amount,0.0),
    nvl(old.order_coupon_reduce_last_30d_amount,0.0)+nvl(1d_ago.order_coupon_reduce_amount,0.0)- nvl(30d_ago.order_coupon_reduce_amount,0.0),
    nvl(old.order_last_30d_original_amount,0.0)+nvl(1d_ago.order_original_amount,0.0)- nvl(30d_ago.order_original_amount,0.0),
    nvl(old.order_last_30d_final_amount,0.0)+nvl(1d_ago.order_final_amount,0.0)- nvl(30d_ago.order_final_amount,0.0),
    nvl(old.order_count,0)+nvl(1d_ago.order_count,0),
    nvl(old.order_num,0)+nvl(1d_ago.order_num,0),
    nvl(old.order_activity_count,0)+nvl(1d_ago.order_activity_count,0),
    nvl(old.order_coupon_count,0)+nvl(1d_ago.order_coupon_count,0),
    nvl(old.order_activity_reduce_amount,0.0)+nvl(1d_ago.order_activity_reduce_amount,0.0),
    nvl(old.order_coupon_reduce_amount,0.0)+nvl(1d_ago.order_coupon_reduce_amount,0.0),
    nvl(old.order_original_amount,0.0)+nvl(1d_ago.order_original_amount,0.0),
    nvl(old.order_final_amount,0.0)+nvl(1d_ago.order_final_amount,0.0),
    nvl(1d_ago.payment_count,0),
    nvl(1d_ago.payment_num,0),
    nvl(1d_ago.payment_amount,0.0),
    nvl(old.payment_last_7d_count,0)+nvl(1d_ago.payment_count,0)- nvl(7d_ago.payment_count,0),
    nvl(old.payment_last_7d_num,0)+nvl(1d_ago.payment_num,0)- nvl(7d_ago.payment_num,0),
    nvl(old.payment_last_7d_amount,0.0)+nvl(1d_ago.payment_amount,0.0)- nvl(7d_ago.payment_amount,0.0),
    nvl(old.payment_last_30d_count,0)+nvl(1d_ago.payment_count,0)- nvl(30d_ago.payment_count,0),
    nvl(old.payment_last_30d_num,0)+nvl(1d_ago.payment_num,0)- nvl(30d_ago.payment_num,0),
    nvl(old.payment_last_30d_amount,0.0)+nvl(1d_ago.payment_amount,0.0)- nvl(30d_ago.payment_amount,0.0),
    nvl(old.payment_count,0)+nvl(1d_ago.payment_count,0),
    nvl(old.payment_num,0)+nvl(1d_ago.payment_num,0),
    nvl(old.payment_amount,0.0)+nvl(1d_ago.payment_amount,0.0),
    nvl(old.refund_order_last_1d_count,0)+nvl(1d_ago.refund_order_count,0)- nvl(1d_ago.refund_order_count,0),
    nvl(old.refund_order_last_1d_num,0)+nvl(1d_ago.refund_order_num,0)- nvl(1d_ago.refund_order_num,0),
    nvl(old.refund_order_last_1d_amount,0.0)+nvl(1d_ago.refund_order_amount,0.0)- nvl(1d_ago.refund_order_amount,0.0),
    nvl(old.refund_order_last_7d_count,0)+nvl(1d_ago.refund_order_count,0)- nvl(7d_ago.refund_order_count,0),
    nvl(old.refund_order_last_7d_num,0)+nvl(1d_ago.refund_order_num,0)- nvl(7d_ago.refund_order_num,0),
    nvl(old.refund_order_last_7d_amount,0.0)+nvl(1d_ago.refund_order_amount,0.0)- nvl(7d_ago.refund_order_amount,0.0),
    nvl(old.refund_order_last_30d_count,0)+nvl(1d_ago.refund_order_count,0)- nvl(30d_ago.refund_order_count,0),
    nvl(old.refund_order_last_30d_num,0)+nvl(1d_ago.refund_order_num,0)- nvl(30d_ago.refund_order_num,0),
    nvl(old.refund_order_last_30d_amount,0.0)+nvl(1d_ago.refund_order_amount,0.0)- nvl(30d_ago.refund_order_amount,0.0),
    nvl(old.refund_order_count,0)+nvl(1d_ago.refund_order_count,0),
    nvl(old.refund_order_num,0)+nvl(1d_ago.refund_order_num,0),
    nvl(old.refund_order_amount,0.0)+nvl(1d_ago.refund_order_amount,0.0),
    nvl(1d_ago.refund_payment_count,0),
    nvl(1d_ago.refund_payment_num,0),
    nvl(1d_ago.refund_payment_amount,0.0),
    nvl(old.refund_payment_last_7d_count,0)+nvl(1d_ago.refund_payment_count,0)- nvl(7d_ago.refund_payment_count,0),
    nvl(old.refund_payment_last_7d_num,0)+nvl(1d_ago.refund_payment_num,0)- nvl(7d_ago.refund_payment_num,0),
    nvl(old.refund_payment_last_7d_amount,0.0)+nvl(1d_ago.refund_payment_amount,0.0)- nvl(7d_ago.refund_payment_amount,0.0),
    nvl(old.refund_payment_last_30d_count,0)+nvl(1d_ago.refund_payment_count,0)- nvl(30d_ago.refund_payment_count,0),
    nvl(old.refund_payment_last_30d_num,0)+nvl(1d_ago.refund_payment_num,0)- nvl(30d_ago.refund_payment_num,0),
    nvl(old.refund_payment_last_30d_amount,0.0)+nvl(1d_ago.refund_payment_amount,0.0)- nvl(30d_ago.refund_payment_amount,0.0),
    nvl(old.refund_payment_count,0)+nvl(1d_ago.refund_payment_count,0),
    nvl(old.refund_payment_num,0)+nvl(1d_ago.refund_payment_num,0),
    nvl(old.refund_payment_amount,0.0)+nvl(1d_ago.refund_payment_amount,0.0),
    nvl(1d_ago.cart_count,0),
    nvl(old.cart_last_7d_count,0)+nvl(1d_ago.cart_count,0)- nvl(7d_ago.cart_count,0),
    nvl(old.cart_last_30d_count,0)+nvl(1d_ago.cart_count,0)- nvl(30d_ago.cart_count,0),
    nvl(old.cart_count,0)+nvl(1d_ago.cart_count,0),
    nvl(1d_ago.favor_count,0),
    nvl(old.favor_last_7d_count,0)+nvl(1d_ago.favor_count,0)- nvl(7d_ago.favor_count,0),
    nvl(old.favor_last_30d_count,0)+nvl(1d_ago.favor_count,0)- nvl(30d_ago.favor_count,0),
    nvl(old.favor_count,0)+nvl(1d_ago.favor_count,0),
    nvl(1d_ago.appraise_good_count,0),
    nvl(1d_ago.appraise_mid_count,0),
    nvl(1d_ago.appraise_bad_count,0),
    nvl(1d_ago.appraise_default_count,0),
    nvl(old.appraise_last_7d_good_count,0)+nvl(1d_ago.appraise_good_count,0)- nvl(7d_ago.appraise_good_count,0),
    nvl(old.appraise_last_7d_mid_count,0)+nvl(1d_ago.appraise_mid_count,0)- nvl(7d_ago.appraise_mid_count,0),
    nvl(old.appraise_last_7d_bad_count,0)+nvl(1d_ago.appraise_bad_count,0)- nvl(7d_ago.appraise_bad_count,0),
    nvl(old.appraise_last_7d_default_count,0)+nvl(1d_ago.appraise_default_count,0)- nvl(7d_ago.appraise_default_count,0),
    nvl(old.appraise_last_30d_good_count,0)+nvl(1d_ago.appraise_good_count,0)- nvl(30d_ago.appraise_good_count,0),
    nvl(old.appraise_last_30d_mid_count,0)+nvl(1d_ago.appraise_mid_count,0)- nvl(30d_ago.appraise_mid_count,0),
    nvl(old.appraise_last_30d_bad_count,0)+nvl(1d_ago.appraise_bad_count,0)- nvl(30d_ago.appraise_bad_count,0),
    nvl(old.appraise_last_30d_default_count,0)+nvl(1d_ago.appraise_default_count,0)- nvl(30d_ago.appraise_default_count,0),
    nvl(old.appraise_good_count,0)+nvl(1d_ago.appraise_good_count,0),
    nvl(old.appraise_mid_count,0)+nvl(1d_ago.appraise_mid_count,0),
    nvl(old.appraise_bad_count,0)+nvl(1d_ago.appraise_bad_count,0),
    nvl(old.appraise_default_count,0)+nvl(1d_ago.appraise_default_count,0)
from
(
    select
        sku_id,
        order_last_1d_count,
        order_last_1d_num,
        order_activity_last_1d_count,
        order_coupon_last_1d_count,
        order_activity_reduce_last_1d_amount,
        order_coupon_reduce_last_1d_amount,
        order_last_1d_original_amount,
        order_last_1d_final_amount,
        order_last_7d_count,
        order_last_7d_num,
        order_activity_last_7d_count,
        order_coupon_last_7d_count,
        order_activity_reduce_last_7d_amount,
        order_coupon_reduce_last_7d_amount,
        order_last_7d_original_amount,
        order_last_7d_final_amount,
        order_last_30d_count,
        order_last_30d_num,
        order_activity_last_30d_count,
        order_coupon_last_30d_count,
        order_activity_reduce_last_30d_amount,
        order_coupon_reduce_last_30d_amount,
        order_last_30d_original_amount,
        order_last_30d_final_amount,
        order_count,
        order_num,
        order_activity_count,
        order_coupon_count,
        order_activity_reduce_amount,
        order_coupon_reduce_amount,
        order_original_amount,
        order_final_amount,
        payment_last_1d_count,
        payment_last_1d_num,
        payment_last_1d_amount,
        payment_last_7d_count,
        payment_last_7d_num,
        payment_last_7d_amount,
        payment_last_30d_count,
        payment_last_30d_num,
        payment_last_30d_amount,
        payment_count,
        payment_num,
        payment_amount,
        refund_order_last_1d_count,
        refund_order_last_1d_num,
        refund_order_last_1d_amount,
        refund_order_last_7d_count,
        refund_order_last_7d_num,
        refund_order_last_7d_amount,
        refund_order_last_30d_count,
        refund_order_last_30d_num,
        refund_order_last_30d_amount,
        refund_order_count,
        refund_order_num,
        refund_order_amount,
        refund_payment_last_1d_count,
        refund_payment_last_1d_num,
        refund_payment_last_1d_amount,
        refund_payment_last_7d_count,
        refund_payment_last_7d_num,
        refund_payment_last_7d_amount,
        refund_payment_last_30d_count,
        refund_payment_last_30d_num,
        refund_payment_last_30d_amount,
        refund_payment_count,
        refund_payment_num,
        refund_payment_amount,
        cart_last_1d_count,
        cart_last_7d_count,
        cart_last_30d_count,
        cart_count,
        favor_last_1d_count,
        favor_last_7d_count,
        favor_last_30d_count,
        favor_count,
        appraise_last_1d_good_count,
        appraise_last_1d_mid_count,
        appraise_last_1d_bad_count,
        appraise_last_1d_default_count,
        appraise_last_7d_good_count,
        appraise_last_7d_mid_count,
        appraise_last_7d_bad_count,
        appraise_last_7d_default_count,
        appraise_last_30d_good_count,
        appraise_last_30d_mid_count,
        appraise_last_30d_bad_count,
        appraise_last_30d_default_count,
        appraise_good_count,
        appraise_mid_count,
        appraise_bad_count,
        appraise_default_count
    from ${APP}.dwt_sku_topic
    where dt=date_add('$do_date',-1)
)old
full outer join
(
    select
        sku_id,
        order_count,
        order_num,
        order_activity_count,
        order_coupon_count,
        order_activity_reduce_amount,
        order_coupon_reduce_amount,
        order_original_amount,
        order_final_amount,
        payment_count,
        payment_num,
        payment_amount,
        refund_order_count,
        refund_order_num,
        refund_order_amount,
        refund_payment_count,
        refund_payment_num,
        refund_payment_amount,
        cart_count,
        favor_count,
        appraise_good_count,
        appraise_mid_count,
        appraise_bad_count,
        appraise_default_count
    from ${APP}.dws_sku_action_daycount
    where dt='$do_date'
)1d_ago
on old.sku_id=1d_ago.sku_id
left join
(
    select
        sku_id,
        order_count,
        order_num,
        order_activity_count,
        order_coupon_count,
        order_activity_reduce_amount,
        order_coupon_reduce_amount,
        order_original_amount,
        order_final_amount,
        payment_count,
        payment_num,
        payment_amount,
        refund_order_count,
        refund_order_num,
        refund_order_amount,
        refund_payment_count,
        refund_payment_num,
        refund_payment_amount,
        cart_count,
        favor_count,
        appraise_good_count,
        appraise_mid_count,
        appraise_bad_count,
        appraise_default_count
    from ${APP}.dws_sku_action_daycount
    where dt=date_add('$do_date',-7)
)7d_ago
on old.sku_id=7d_ago.sku_id
left join
(
    select
        sku_id,
        order_count,
        order_num,
        order_activity_count,
        order_coupon_count,
        order_activity_reduce_amount,
        order_coupon_reduce_amount,
        order_original_amount,
        order_final_amount,
        payment_count,
        payment_num,
        payment_amount,
        refund_order_count,
        refund_order_num,
        refund_order_amount,
        refund_payment_count,
        refund_payment_num,
        refund_payment_amount,
        cart_count,
        favor_count,
        appraise_good_count,
        appraise_mid_count,
        appraise_bad_count,
        appraise_default_count
    from ${APP}.dws_sku_action_daycount
    where dt=date_add('$do_date',-30)
)30d_ago
on old.sku_id=30d_ago.sku_id;
alter table ${APP}.dwt_sku_topic drop partition(dt='$clear_date');
"

dwt_activity_topic="
insert overwrite table ${APP}.dwt_activity_topic partition(dt='$do_date')
select
    nvl(1d_ago.activity_rule_id,old.activity_rule_id),
    nvl(1d_ago.activity_id,old.activity_id),
    nvl(1d_ago.order_count,0),
    nvl(1d_ago.order_reduce_amount,0.0),
    nvl(1d_ago.order_original_amount,0.0),
    nvl(1d_ago.order_final_amount,0.0),
    nvl(old.order_count,0)+nvl(1d_ago.order_count,0),
    nvl(old.order_reduce_amount,0.0)+nvl(1d_ago.order_reduce_amount,0.0),
    nvl(old.order_original_amount,0.0)+nvl(1d_ago.order_original_amount,0.0),
    nvl(old.order_final_amount,0.0)+nvl(1d_ago.order_final_amount,0.0),
    nvl(1d_ago.payment_count,0),
    nvl(1d_ago.payment_reduce_amount,0.0),
    nvl(1d_ago.payment_amount,0.0),
    nvl(old.payment_count,0)+nvl(1d_ago.payment_count,0),
    nvl(old.payment_reduce_amount,0.0)+nvl(1d_ago.payment_reduce_amount,0.0),
    nvl(old.payment_amount,0.0)+nvl(1d_ago.payment_amount,0.0)
from
(
    select
        activity_rule_id,
        activity_id,
        order_count,
        order_reduce_amount,
        order_original_amount,
        order_final_amount,
        payment_count,
        payment_reduce_amount,
        payment_amount
    from ${APP}.dwt_activity_topic
    where dt=date_add('$do_date',-1)
)old
full outer join
(
    select
        activity_rule_id,
        activity_id,
        order_count,
        order_reduce_amount,
        order_original_amount,
        order_final_amount,
        payment_count,
        payment_reduce_amount,
        payment_amount
    from ${APP}.dws_activity_info_daycount
    where dt='$do_date'
)1d_ago
on old.activity_rule_id=1d_ago.activity_rule_id;
alter table ${APP}.dwt_activity_topic drop partition(dt='$clear_date');
"

dwt_coupon_topic="
insert overwrite table ${APP}.dwt_coupon_topic partition(dt='$do_date')
select
    nvl(1d_ago.coupon_id,old.coupon_id),
    nvl(1d_ago.get_count,0),
    nvl(old.get_last_7d_count,0)+nvl(1d_ago.get_count,0)- nvl(7d_ago.get_count,0),
    nvl(old.get_last_30d_count,0)+nvl(1d_ago.get_count,0)- nvl(30d_ago.get_count,0),
    nvl(old.get_count,0)+nvl(1d_ago.get_count,0),
    nvl(1d_ago.order_count,0),
    nvl(1d_ago.order_reduce_amount,0.0),
    nvl(1d_ago.order_original_amount,0.0),
    nvl(1d_ago.order_final_amount,0.0),
    nvl(old.order_last_7d_count,0)+nvl(1d_ago.order_count,0)- nvl(7d_ago.order_count,0),
    nvl(old.order_last_7d_reduce_amount,0.0)+nvl(1d_ago.order_reduce_amount,0.0)- nvl(7d_ago.order_reduce_amount,0.0),
    nvl(old.order_last_7d_original_amount,0.0)+nvl(1d_ago.order_original_amount,0.0)- nvl(7d_ago.order_original_amount,0.0),
    nvl(old.order_last_7d_final_amount,0.0)+nvl(1d_ago.order_final_amount,0.0)- nvl(7d_ago.order_final_amount,0.0),
    nvl(old.order_last_30d_count,0)+nvl(1d_ago.order_count,0)- nvl(30d_ago.order_count,0),
    nvl(old.order_last_30d_reduce_amount,0.0)+nvl(1d_ago.order_reduce_amount,0.0)- nvl(30d_ago.order_reduce_amount,0.0),
    nvl(old.order_last_30d_original_amount,0.0)+nvl(1d_ago.order_original_amount,0.0)- nvl(30d_ago.order_original_amount,0.0),
    nvl(old.order_last_30d_final_amount,0.0)+nvl(1d_ago.order_final_amount,0.0)- nvl(30d_ago.order_final_amount,0.0),
    nvl(old.order_count,0)+nvl(1d_ago.order_count,0),
    nvl(old.order_reduce_amount,0.0)+nvl(1d_ago.order_reduce_amount,0.0),
    nvl(old.order_original_amount,0.0)+nvl(1d_ago.order_original_amount,0.0),
    nvl(old.order_final_amount,0.0)+nvl(1d_ago.order_final_amount,0.0),
    nvl(old.payment_last_1d_count,0)+nvl(1d_ago.payment_count,0)- nvl(1d_ago.payment_count,0),
    nvl(old.payment_last_1d_reduce_amount,0.0)+nvl(1d_ago.payment_reduce_amount,0.0)- nvl(1d_ago.payment_reduce_amount,0.0),
    nvl(old.payment_last_1d_amount,0.0)+nvl(1d_ago.payment_amount,0.0)- nvl(1d_ago.payment_amount,0.0),
    nvl(old.payment_last_7d_count,0)+nvl(1d_ago.payment_count,0)- nvl(7d_ago.payment_count,0),
    nvl(old.payment_last_7d_reduce_amount,0.0)+nvl(1d_ago.payment_reduce_amount,0.0)- nvl(7d_ago.payment_reduce_amount,0.0),
    nvl(old.payment_last_7d_amount,0.0)+nvl(1d_ago.payment_amount,0.0)- nvl(7d_ago.payment_amount,0.0),
    nvl(old.payment_last_30d_count,0)+nvl(1d_ago.payment_count,0)- nvl(30d_ago.payment_count,0),
    nvl(old.payment_last_30d_reduce_amount,0.0)+nvl(1d_ago.payment_reduce_amount,0.0)- nvl(30d_ago.payment_reduce_amount,0.0),
    nvl(old.payment_last_30d_amount,0.0)+nvl(1d_ago.payment_amount,0.0)- nvl(30d_ago.payment_amount,0.0),
    nvl(old.payment_count,0)+nvl(1d_ago.payment_count,0),
    nvl(old.payment_reduce_amount,0.0)+nvl(1d_ago.payment_reduce_amount,0.0),
    nvl(old.payment_amount,0.0)+nvl(1d_ago.payment_amount,0.0),
    nvl(1d_ago.expire_count,0),
    nvl(old.expire_last_7d_count,0)+nvl(1d_ago.expire_count,0)- nvl(7d_ago.expire_count,0),
    nvl(old.expire_last_30d_count,0)+nvl(1d_ago.expire_count,0)- nvl(30d_ago.expire_count,0),
    nvl(old.expire_count,0)+nvl(1d_ago.expire_count,0)
from
(
    select
        coupon_id,
        get_last_1d_count,
        get_last_7d_count,
        get_last_30d_count,
        get_count,
        order_last_1d_count,
        order_last_1d_reduce_amount,
        order_last_1d_original_amount,
        order_last_1d_final_amount,
        order_last_7d_count,
        order_last_7d_reduce_amount,
        order_last_7d_original_amount,
        order_last_7d_final_amount,
        order_last_30d_count,
        order_last_30d_reduce_amount,
        order_last_30d_original_amount,
        order_last_30d_final_amount,
        order_count,
        order_reduce_amount,
        order_original_amount,
        order_final_amount,
        payment_last_1d_count,
        payment_last_1d_reduce_amount,
        payment_last_1d_amount,
        payment_last_7d_count,
        payment_last_7d_reduce_amount,
        payment_last_7d_amount,
        payment_last_30d_count,
        payment_last_30d_reduce_amount,
        payment_last_30d_amount,
        payment_count,
        payment_reduce_amount,
        payment_amount,
        expire_last_1d_count,
        expire_last_7d_count,
        expire_last_30d_count,
        expire_count
    from ${APP}.dwt_coupon_topic
    where dt=date_add('$do_date',-1)
)old
full outer join
(
    select
        coupon_id,
        get_count,
        order_count,
        order_reduce_amount,
        order_original_amount,
        order_final_amount,
        payment_count,
        payment_reduce_amount,
        payment_amount,
        expire_count
    from ${APP}.dws_coupon_info_daycount
    where dt='$do_date'
)1d_ago
on old.coupon_id=1d_ago.coupon_id
left join
(
    select
        coupon_id,
        get_count,
        order_count,
        order_reduce_amount,
        order_original_amount,
        order_final_amount,
        payment_count,
        payment_reduce_amount,
        payment_amount,
        expire_count
    from ${APP}.dws_coupon_info_daycount
    where dt=date_add('$do_date',-7)
)7d_ago
on old.coupon_id=7d_ago.coupon_id
left join
(
    select
        coupon_id,
        get_count,
        order_count,
        order_reduce_amount,
        order_original_amount,
        order_final_amount,
        payment_count,
        payment_reduce_amount,
        payment_amount,
        expire_count
    from ${APP}.dws_coupon_info_daycount
    where dt=date_add('$do_date',-30)
)30d_ago
on old.coupon_id=30d_ago.coupon_id;
alter table ${APP}.dwt_coupon_topic drop partition(dt='$clear_date');
"

dwt_area_topic="
insert overwrite table ${APP}.dwt_area_topic partition(dt='$do_date')
select
    nvl(old.province_id, 1d_ago.province_id),
    nvl(1d_ago.visit_count,0),
    nvl(1d_ago.login_count,0),
    nvl(old.visit_last_7d_count,0)+nvl(1d_ago.visit_count,0)- nvl(7d_ago.visit_count,0),
    nvl(old.login_last_7d_count,0)+nvl(1d_ago.login_count,0)- nvl(7d_ago.login_count,0),
    nvl(old.visit_last_30d_count,0)+nvl(1d_ago.visit_count,0)- nvl(30d_ago.visit_count,0),
    nvl(old.login_last_30d_count,0)+nvl(1d_ago.login_count,0)- nvl(30d_ago.login_count,0),
    nvl(old.visit_count,0)+nvl(1d_ago.visit_count,0),
    nvl(old.login_count,0)+nvl(1d_ago.login_count,0),
    nvl(1d_ago.order_count,0),
    nvl(1d_ago.order_original_amount,0.0),
    nvl(1d_ago.order_final_amount,0.0),
    nvl(old.order_last_7d_count,0)+nvl(1d_ago.order_count,0)- nvl(7d_ago.order_count,0),
    nvl(old.order_last_7d_original_amount,0.0)+nvl(1d_ago.order_original_amount,0.0)- nvl(7d_ago.order_original_amount,0.0),
    nvl(old.order_last_7d_final_amount,0.0)+nvl(1d_ago.order_final_amount,0.0)- nvl(7d_ago.order_final_amount,0.0),
    nvl(old.order_last_30d_count,0)+nvl(1d_ago.order_count,0)- nvl(30d_ago.order_count,0),
    nvl(old.order_last_30d_original_amount,0.0)+nvl(1d_ago.order_original_amount,0.0)- nvl(30d_ago.order_original_amount,0.0),
    nvl(old.order_last_30d_final_amount,0.0)+nvl(1d_ago.order_final_amount,0.0)- nvl(30d_ago.order_final_amount,0.0),
    nvl(old.order_count,0)+nvl(1d_ago.order_count,0),
    nvl(old.order_original_amount,0.0)+nvl(1d_ago.order_original_amount,0.0),
    nvl(old.order_final_amount,0.0)+nvl(1d_ago.order_final_amount,0.0),
    nvl(1d_ago.payment_count,0),
    nvl(1d_ago.payment_amount,0.0),
    nvl(old.payment_last_7d_count,0)+nvl(1d_ago.payment_count,0)- nvl(7d_ago.payment_count,0),
    nvl(old.payment_last_7d_amount,0.0)+nvl(1d_ago.payment_amount,0.0)- nvl(7d_ago.payment_amount,0.0),
    nvl(old.payment_last_30d_count,0)+nvl(1d_ago.payment_count,0)- nvl(30d_ago.payment_count,0),
    nvl(old.payment_last_30d_amount,0.0)+nvl(1d_ago.payment_amount,0.0)- nvl(30d_ago.payment_amount,0.0),
    nvl(old.payment_count,0)+nvl(1d_ago.payment_count,0),
    nvl(old.payment_amount,0.0)+nvl(1d_ago.payment_amount,0.0),
    nvl(1d_ago.refund_order_count,0),
    nvl(1d_ago.refund_order_amount,0.0),
    nvl(old.refund_order_last_7d_count,0)+nvl(1d_ago.refund_order_count,0)- nvl(7d_ago.refund_order_count,0),
    nvl(old.refund_order_last_7d_amount,0.0)+nvl(1d_ago.refund_order_amount,0.0)- nvl(7d_ago.refund_order_amount,0.0),
    nvl(old.refund_order_last_30d_count,0)+nvl(1d_ago.refund_order_count,0)- nvl(30d_ago.refund_order_count,0),
    nvl(old.refund_order_last_30d_amount,0.0)+nvl(1d_ago.refund_order_amount,0.0)- nvl(30d_ago.refund_order_amount,0.0),
    nvl(old.refund_order_count,0)+nvl(1d_ago.refund_order_count,0),
    nvl(old.refund_order_amount,0.0)+nvl(1d_ago.refund_order_amount,0.0),
    nvl(1d_ago.refund_payment_count,0),
    nvl(1d_ago.refund_payment_amount,0.0),
    nvl(old.refund_payment_last_7d_count,0)+nvl(1d_ago.refund_payment_count,0)- nvl(7d_ago.refund_payment_count,0),
    nvl(old.refund_payment_last_7d_amount,0.0)+nvl(1d_ago.refund_payment_amount,0.0)- nvl(7d_ago.refund_payment_amount,0.0),
    nvl(old.refund_payment_last_30d_count,0)+nvl(1d_ago.refund_payment_count,0)- nvl(30d_ago.refund_payment_count,0),
    nvl(old.refund_payment_last_30d_amount,0.0)+nvl(1d_ago.refund_payment_amount,0.0)- nvl(30d_ago.refund_payment_amount,0.0),
    nvl(old.refund_payment_count,0)+nvl(1d_ago.refund_payment_count,0),
    nvl(old.refund_payment_amount,0.0)+nvl(1d_ago.refund_payment_amount,0.0)

from
(
    select
        province_id,
        visit_last_1d_count,
        login_last_1d_count,
        visit_last_7d_count,
        login_last_7d_count,
        visit_last_30d_count,
        login_last_30d_count,
        visit_count,
        login_count,
        order_last_1d_count,
        order_last_1d_original_amount,
        order_last_1d_final_amount,
        order_last_7d_count,
        order_last_7d_original_amount,
        order_last_7d_final_amount,
        order_last_30d_count,
        order_last_30d_original_amount,
        order_last_30d_final_amount,
        order_count,
        order_original_amount,
        order_final_amount,
        payment_last_1d_count,
        payment_last_1d_amount,
        payment_last_7d_count,
        payment_last_7d_amount,
        payment_last_30d_count,
        payment_last_30d_amount,
        payment_count,
        payment_amount,
        refund_order_last_1d_count,
        refund_order_last_1d_amount,
        refund_order_last_7d_count,
        refund_order_last_7d_amount,
        refund_order_last_30d_count,
        refund_order_last_30d_amount,
        refund_order_count,
        refund_order_amount,
        refund_payment_last_1d_count,
        refund_payment_last_1d_amount,
        refund_payment_last_7d_count,
        refund_payment_last_7d_amount,
        refund_payment_last_30d_count,
        refund_payment_last_30d_amount,
        refund_payment_count,
        refund_payment_amount
    from ${APP}.dwt_area_topic
    where dt=date_add('$do_date',-1)
)old
full outer join
(
    select
        province_id,
        visit_count,
        login_count,
        order_count,
        order_original_amount,
        order_final_amount,
        payment_count,
        payment_amount,
        refund_order_count,
        refund_order_amount,
        refund_payment_count,
        refund_payment_amount
    from ${APP}.dws_area_stats_daycount
    where dt='$do_date'
)1d_ago
on old.province_id=1d_ago.province_id
left join
(
    select
        province_id,
        visit_count,
        login_count,
        order_count,
        order_original_amount,
        order_final_amount,
        payment_count,
        payment_amount,
        refund_order_count,
        refund_order_amount,
        refund_payment_count,
        refund_payment_amount
    from ${APP}.dws_area_stats_daycount
    where dt=date_add('$do_date',-7)
)7d_ago
on old.province_id= 7d_ago.province_id
left join
(
    select
        province_id,
        visit_count,
        login_count,
        order_count,
        order_original_amount,
        order_final_amount,
        payment_count,
        payment_amount,
        refund_order_count,
        refund_order_amount,
        refund_payment_count,
        refund_payment_amount
    from ${APP}.dws_area_stats_daycount
    where dt=date_add('$do_date',-30)
)30d_ago
on old.province_id= 30d_ago.province_id;
alter table ${APP}.dwt_area_topic drop partition(dt='$clear_date');
"


case $1 in
    "dwt_visitor_topic" )
        hive -e "$dwt_visitor_topic"
        hadoop fs -rm -r -f /warehouse/gmall/dwt/dwt_visitor_topic/dt=$clear_date
    ;;
    "dwt_user_topic" )
        hive -e "$dwt_user_topic"
        hadoop fs -rm -r -f /warehouse/gmall/dwt/dwt_user_topic/dt=$clear_date
    ;;
    "dwt_sku_topic" )
        hive -e "$dwt_sku_topic"
        hadoop fs -rm -r -f /warehouse/gmall/dwt/dwt_sku_topic/dt=$clear_date
    ;;
    "dwt_activity_topic" )
        hive -e "$dwt_activity_topic"
        hadoop fs -rm -r -f /warehouse/gmall/dwt/dwt_activity_topic/dt=$clear_date
    ;;
    "dwt_coupon_topic" )
        hive -e "$dwt_coupon_topic"
        hadoop fs -rm -r -f /warehouse/gmall/dwt/dwt_coupon_topic/dt=$clear_date
    ;;
    "dwt_area_topic" )
        hive -e "$dwt_area_topic"
        hadoop fs -rm -r -f /warehouse/gmall/dwt/dwt_area_topic/dt=$clear_date
    ;;
    "all" )
        hive -e "$dwt_visitor_topic$dwt_user_topic$dwt_sku_topic$dwt_activity_topic$dwt_coupon_topic$dwt_area_topic"
        hadoop fs -rm -r -f /warehouse/gmall/dwt/dwt_visitor_topic/dt=$clear_date
        hadoop fs -rm -r -f /warehouse/gmall/dwt/dwt_user_topic/dt=$clear_date
        hadoop fs -rm -r -f /warehouse/gmall/dwt/dwt_sku_topic/dt=$clear_date
        hadoop fs -rm -r -f /warehouse/gmall/dwt/dwt_activity_topic/dt=$clear_date
        hadoop fs -rm -r -f /warehouse/gmall/dwt/dwt_coupon_topic/dt=$clear_date
        hadoop fs -rm -r -f /warehouse/gmall/dwt/dwt_area_topic/dt=$clear_date
    ;;
esac

```



# 问题

1. hdfs web无法预览文件
2. snn：9868无法访问
3. shell脚本如何去除空格
