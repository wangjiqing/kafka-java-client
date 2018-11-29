package com.sakura.spark.project.spark

import com.sakura.spark.project.domain.ClickLog
import com.sakura.spark.project.utils.DateUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用Spark Streaming处理Kafka收集的数据
  * hadoop000:2181 test streamingtopic 1
  */
object StatStreamingApp {
  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      println("Usage: StatStreamingApp <zkQuorum> <groupId> <topics> <numThreads>")
      System.exit(1)
    }

    val Array(zkQuorum, groupId, topics, numThreads) = args

    val sparkConf = new SparkConf().setAppName("StatStreamingApp").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(60))

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val messages = KafkaUtils.createStream(ssc, zkQuorum, groupId, topicMap)

    // 数据清洗，取得日志中保存的课程编号
    // 30.143.156.29	2018-11-29 16:28:01	GET /class/128.html HTTP/1.1	404	https://www.baidu.com/s?wd=Spark Streaming实战
    val logs = messages.map(_._2)
    val cleanData = logs.map(line => {
      // 用一个制表符\t 切割一行日志
      val infos = line.split("\t")
      // 从切割出来的infos数组中，取得下标为2元素为访问地址，使用" "分割，取下标为1的元素，为访问的地址
      val url = infos(2).split(" ")(1)
      // 判断实战课程为/class开头的课程，使用"/"分割，取得下标为2的元素，为课程编号
      var courseId = 0
      if (url.startsWith("/class")) {
        val courseIdHTML = url.split("/")(2)
        courseId = courseIdHTML.substring(0, courseIdHTML.lastIndexOf(".")).toInt
      }
      // 放在ClickLog对象里
      ClickLog(infos(0), DateUtils.parseToMinute(infos(1)), courseId, infos(3).toInt, infos(4))
    }).filter(clicklog => clicklog.courseId != 0)

    cleanData.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
