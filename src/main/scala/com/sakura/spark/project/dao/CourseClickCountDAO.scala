package com.sakura.spark.project.dao

import com.sakura.spark.kafka.project.utils.HBaseUtils
import com.sakura.spark.project.domain.CourseClickCount
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/**
  * 实战课程点击数数据访问层
  */
object CourseClickCountDAO {

  val tableName = "imooc_course_clickcount"
  val cf = "info"
  val qualifer = "click_count"

  /**
    * 保存数据到HBase
    * @param CourseClickCount集合
    */
  def save(list: ListBuffer[CourseClickCount]): Unit = {

    val table = HBaseUtils.getInstance().getTable(tableName)

    for (ele <- list) {
      table.incrementColumnValue(Bytes.toBytes(ele.day_course),
        Bytes.toBytes(cf),
        Bytes.toBytes(qualifer),
        ele.click_count)
    }

  }

  def count(day_course: String):Long = {
    val table = HBaseUtils.getInstance().getTable(tableName)

    val get = new Get(day_course.getBytes())
    val value = table.get(get).getValue(cf.getBytes, qualifer.getBytes)

    if (value == null) {
      0L
    } else {
      Bytes.toLong(value)
    }
  }

  def main(args: Array[String]): Unit = {
    val list = new ListBuffer[CourseClickCount]
    list.append(CourseClickCount("29171111_8", 20))
    list.append(CourseClickCount("29171111_9", 30))
    list.append(CourseClickCount("29171111_1", 199))

    save(list)
  }
}
