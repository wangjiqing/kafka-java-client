package com.sakura.spark.streamingkafka

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Spark Streaming对接Kafka
  * 本地测试参数：hadoop000:2181 test streamingtopic 1
  */
object KafkaStreamingApp {
  def main(args: Array[String]): Unit = {
    // 测试参数：hadoop000:2181 test streamingtopic 1
    if (args.length != 4) {
      println("Userage: KafkaStreamingApp <zkQuorum> <group> <topics> <numThreads>")
    }
    val Array(zkQuorum, group, topics, numThreads) = args
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    val sparkConf = new SparkConf().setAppName("KafkaStreamingApp").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val messages = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap)
    messages.map(_._2).count().print()

    ssc.start()
    ssc.awaitTermination()
  }
}