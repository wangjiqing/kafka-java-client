# 基于kafka_2.11_0.9.0.0的生产者消费者 Java客户端

详细内容见[代码](https://github.com/wangjiqing/sparktrain/tree/master/src/main/java/com/sakura/spark/kafka)

# 基于Spark Streaming 2.11(Scala) 2.2.0 的实例开发

1. [NetworkWordCount](https://github.com/wangjiqing/sparktrain/blob/master/src/main/scala/com/sakura/spark/app/NetworkWordCount.scala) - tcp套接字监听，计算wordcount

2. [FileWordCount](https://github.com/wangjiqing/sparktrain/blob/master/src/main/scala/com/sakura/spark/app/FileWordCount.scala) - 监听文件夹日志文件
    
3. [StatefulWordCount](https://github.com/wangjiqing/sparktrain/blob/master/src/main/scala/com/sakura/spark/app/StatefulWordCount.scala) - 有状态的统计
    
4. [ForeachRDDApp](https://github.com/wangjiqing/sparktrain/blob/master/src/main/scala/com/sakura/spark/app/ForeachRDDApp.scala) - 通过SQL将统计数据写入MySQL
    
5. [WindowWordCount](https://github.com/wangjiqing/sparktrain/blob/master/src/main/scala/com/sakura/spark/app/WindowWordCount.scala) - 窗口计数统计
    
6. [TransformApp](https://github.com/wangjiqing/sparktrain/blob/master/src/main/scala/com/sakura/spark/app/TransformApp.scala) - 使用Transform转换DStream,实现黑名单过滤功能
    
7. [SqlNetworkWordCount](https://github.com/wangjiqing/sparktrain/blob/master/src/main/scala/com/sakura/spark/app/SqlNetworkWordCount.scala) - 使用Spark Streaming整合Spark SQL实例
    
# Spark Streaming整合Flume实例代码

1. [FlumePushWordCount](https://github.com/wangjiqing/sparktrain/blob/master/src/main/scala/com/sakura/spark/flume/FlumePushWordCount.scala) - Push方式整合代码示例（Flume主动推送）
    
2. [FlumePollWordCount](https://github.com/wangjiqing/sparktrain/blob/master/src/main/scala/com/sakura/spark/flume/FlumePollWordCount.scala) - Poll方式整合代码示例（Streaming应用程序主动拉取 【事务性，工作中常用此方法】）
    
# Spark Streaming整合Kafka实例代码

1. [KafkaReceiveWordCount](https://github.com/wangjiqing/sparktrain/blob/master/src/main/scala/com/sakura/spark/kafka/KafkaReceiverWordCount.scala) - spark streaming 对接Kafka 1 -- receiver （基于zookeeper）
    
2. [KafkaDirectWordCount](https://github.com/wangjiqing/sparktrain/blob/master/src/main/scala/com/sakura/spark/kafka/KafkaDirectWordCount.scala) - spark streaming 对接Kafka 2 -- direct（工作中基本上使用这个）

# Spark Streaming的模拟日志产生的代码（使用log4j）

1. [LoggerGenerator](https://github.com/wangjiqing/sparktrain/blob/master/src/test/java/LoggerGenerator.java) - main

2. [log4j.properties](https://github.com/wangjiqing/sparktrain/blob/master/src/test/resources/log4j.properties) - log4j配置文件

# spark streaming 接入kafka的代码

1. [KafkaStreamingApp](https://github.com/wangjiqing/sparktrain/blob/master/src/main/scala/com/sakura/spark/streamingkafka/KafkaStreamingApp.scala) - spark streaming 接入kafka的代码

# Spark Streaming实现的project实战使用的demo

1. [StatStreamingApp](https://github.com/wangjiqing/sparktrain/blob/master/src/main/scala/com/sakura/spark/project/spark/StatStreamingApp.scala) - spark streaming 执行主类

2. [DateUtils](https://github.com/wangjiqing/sparktrain/blob/master/src/main/scala/com/sakura/spark/project/utils/DateUtils.scala) - 时间处理工具类

3. [ClickLog](https://github.com/wangjiqing/sparktrain/blob/master/src/main/scala/com/sakura/spark/project/domain/ClickLog.scala) - 保存课程的实体类

4. [CourseClickCount](https://github.com/wangjiqing/sparktrain/blob/master/src/main/scala/com/sakura/spark/project/domain/CourseClickCount.scala) - 战课程点击数实体类

5. [CourseClickCountDAO](https://github.com/wangjiqing/sparktrain/blob/master/src/main/scala/com/sakura/spark/project/dao/CourseClickCountDAO.scala) - 战课程点击数数据库访问Dao层方法

6. [HBaseUtils](https://github.com/wangjiqing/sparktrain/blob/master/src/main/java/com/sakura/spark/kafka/project/utils/HBaseUtils.java) - Java访问HBase客户端工具类

7. [CourseSearchClickCount](https://github.com/wangjiqing/sparktrain/blob/master/src/main/scala/com/sakura/spark/project/domain/CourseSearchClickCount.scala) - 从搜索引擎引流过来的实战课程点击数实体类

8. [CourseSearchClickCountDAO](https://github.com/wangjiqing/sparktrain/blob/master/src/main/scala/com/sakura/spark/project/dao/CourseSearchClickCountDAO.scala) - 从搜索引擎引流过来的实战课程点击数数据访问层

# 扩展使用Java语言开发Spark Core与Spark Streaming应用程序

1. [WordCountApp](https://github.com/wangjiqing/sparktrain/blob/ce88e4c8fd0a82c0906e20726d25b482fbe51d50/src/main/java/com/sakura/spark/kafka/WordCountApp.java) - 使用Java语言开发Spark Core应用程序

2. [StreamingWordCountApp](https://github.com/wangjiqing/sparktrain/blob/ce88e4c8fd0a82c0906e20726d25b482fbe51d50/src/main/java/com/sakura/spark/kafka/StreamingWordCountApp.java) - 使用Java语言开发Spark Streaming应用程序