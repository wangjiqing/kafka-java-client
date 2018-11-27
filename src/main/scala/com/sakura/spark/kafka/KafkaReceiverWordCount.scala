package com.sakura.spark.kafka

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * spark streaming 对接Kafka 1 -- receiver
  */
object KafkaReceiverWordCount {
  def main(args: Array[String]): Unit = {
    // 测试参数：hadoop000:2181 test kafka-streaming-topic 1
    if (args.length != 4) {
      println("Userage: KafkaReceiverWordCount <zkQuorum> <group> <topics> <numThreads>")
    }
    val Array(zkQuorum, group, topics, numThreads) = args
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    val sparkConf = new SparkConf().setAppName("KafkaReceiverWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val messages = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap);
    messages.map(_._2).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).print()

    ssc.start()
    ssc.awaitTermination()
  }
}