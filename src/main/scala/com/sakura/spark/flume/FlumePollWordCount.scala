package com.sakura.spark.flume

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.flume.FlumeUtils

/**
  * Spark Streaming 整合Flume 2
  * 本地设置参数 hadoop000 41414
  */
object FlumePollWordCount {
  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      println("Usage: FlumePullWOrdCount <hostname> : <port>")
      System.exit(1)
    }

    val Array(hostname, port) = args

    val sparkConf = new SparkConf().setAppName("FlumePollWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val flumeStream = FlumeUtils.createPollingStream(ssc, hostname, port.toInt)
    flumeStream.map(x => new String(x.event.getBody.array()).trim)
      .flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
