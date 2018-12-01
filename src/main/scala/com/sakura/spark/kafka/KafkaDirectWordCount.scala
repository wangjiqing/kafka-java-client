package com.sakura.spark.kafka

//import org.apache.batik.util.io.StringDecoder
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.kafka.KafkaUtils
//import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * spark streaming 对接Kafka 2 -- direct
  */
object KafkaDirectWordCount {
//  def main(args: Array[String]): Unit = {
//    // 测试参数：hadoop000:9092 kafka-streaming-topic
//    if (args.length != 2) {
//      println("Userage: KafkaDirectWordCount <borkers> <topics>")
//      System.exit(1)
//    }
//    val Array(borkers, topics) = args
//
//    val sparkConf = new SparkConf().setAppName("KafkaReceiverWordCount").setMaster("local[2]")
//    val ssc = new StreamingContext(sparkConf, Seconds(5))
//
//    val topicSet = topics.split(",").toSet
//    val kafkaParams = Map[String, String]("metadata.broker.list" -> borkers)
//
//    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
//      ssc,
//      kafkaParams,
//      topicSet
//      )
//    messages.map(_._2).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).print()
//
//    ssc.start()
//    ssc.awaitTermination()
//  }
}