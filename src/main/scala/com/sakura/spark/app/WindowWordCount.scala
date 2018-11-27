package com.sakura.spark.app

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Spark Streming window 代码测试
  *
  * 测试方法采用nc lk 6789 测试
  */
object WindowWOrdCount {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("WindowWOrdCount")

    // 创建StreamingContext 两个参数
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val lines = ssc.socketTextStream("localhost", 6789)

    val result = lines.flatMap(_.split(" ")).map((_, 1))
      .reduceByKeyAndWindow((a: Int, b: Int) => (a + b), Seconds(30), Seconds(10));

    result.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
