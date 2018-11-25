package com.sakura.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用Spark Streaming处理文件系统的数据
  */
object FileWordCount {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("FileWordCount").setMaster("local")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val lines = ssc.textFileStream("E:/docker/")

    val result = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    result.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
