package com.sakura.spark.project.dao

import com.sakura.spark.kafka.project.utils.HBaseUtils
import com.sakura.spark.project.domain.CourseSearchClickCount
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/**
  * 从搜索引擎引流过来的实战课程点击数数据访问层
  */
object CourseSearchClickCountDAO {

  val tableName = "imooc_course_search_clickcount"
  val cf = "info"
  val qualifer = "click_count"

  /**
    * 保存数据到HBase
    * @param CourseSearchClickCount
    */
  def save(list: ListBuffer[CourseSearchClickCount]): Unit = {

    val table = HBaseUtils.getInstance().getTable(tableName)

    for (ele <- list) {
      table.incrementColumnValue(Bytes.toBytes(ele.day_search_course),
        Bytes.toBytes(cf),
        Bytes.toBytes(qualifer),
        ele.click_count)
    }

  }

  /**
    * 查询数量
    * @param day_course
    * @return
    */
  def count(day_search_course: String):Long = {
    val table = HBaseUtils.getInstance().getTable(tableName)

    val get = new Get(day_search_course.getBytes())
    val value = table.get(get).getValue(cf.getBytes, qualifer.getBytes)

    if (value == null) {
      0L
    } else {
      Bytes.toLong(value)
    }
  }

  def main(args: Array[String]): Unit = {
    val list = new ListBuffer[CourseSearchClickCount]
    list.append(CourseSearchClickCount("29171111_www.baidu.com_8", 8))
    list.append(CourseSearchClickCount("29171111_cn.bing.com_9", 9))

//    save(list)
    println(count("29171111_www.baidu.com_8") + " : " + count("29171111_cn.bing.com_9"))
  }
}
