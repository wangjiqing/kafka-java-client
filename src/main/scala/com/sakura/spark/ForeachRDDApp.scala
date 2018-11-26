package com.sakura.spark

import java.sql.DriverManager

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用Spark Streaming词频统计，将结果写入MySQL数据库
  */
object ForeachRDDApp {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("ForeachRDDApp").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // 如果使用stateful的算子，必须要设置checkpoint
    // 在生产环境中，建议把checkpoint设置到HDFS的某个文件夹中
    ssc.checkpoint(".")

    val lines = ssc.socketTextStream("localhost", 6789)
    val result = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    // TODO 将结果写入MySQL
    result.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        val connection = createConnection()
        partitionOfRecords.foreach(record => {
          val sql = "insert into wordcount(word, wordcount) values ('"+ record._1 +"', '"+ record._2 +"')"
          connection.createStatement().execute(sql)
        })
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 获取MySQL连接
    * @return
    */
  def createConnection() = {
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://192.168.177.129:3306/test", "root", "chang")
  }
}
