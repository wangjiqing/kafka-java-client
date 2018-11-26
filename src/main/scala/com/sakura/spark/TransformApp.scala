package com.sakura.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用transport实现黑名单过滤的功能
  */
object TransformApp {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("TransformApp")

    // 创建StreamingContext 两个参数
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // 构建黑名单(通常情况下是从数据库中查询)
    val blacks = List("zhangsan", "lisi")
    val blacksRDD = ssc.sparkContext.parallelize(blacks).map(x => (x, true))

    val lines = ssc.socketTextStream("localhost", 6789)

    /**
      * 1. 通过日志进入转换为DStream，这里我们给出日志格式如下：
      * 20180808,shangsan
      * 20180808,lisi
      * 20180808,wangwu
      * 2. 首先将日志转化为
      * (zhangsan: (20180808,shangsan)) (lisi: (20180808,lisi)) (wangwu: (20180808,wangwu))
      * 3. 定义黑名单RDD
      * 例如 黑名单中包括 (zhangsan,lisi) 将每个元素转为map
      * （zhangsan: true） (lisi: true)
      * 4. 最终要得到的结果显而易见，如下：
      * 20180808,wangwu
      * 5. 我们使用leftjoin将转换过来的DStream与RDD（黑名单做join），得到如下格式：
      * (zhangsan: [(20180808,shangsan), true]) (lisi: [(20180808,lisi), true]) (wangwu: [(20180808,wangwu), false])
      * 6. 然后取得最终DStream中的tuple 1
      */
    val clicklog = lines.map(x => (x.split(",")(1), x)).transform(rdd => {
      rdd.leftOuterJoin(blacksRDD)
         .filter(x => x._2._2.getOrElse(false) != true)
         .map(x => x._2._1)
    })

    clicklog.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
