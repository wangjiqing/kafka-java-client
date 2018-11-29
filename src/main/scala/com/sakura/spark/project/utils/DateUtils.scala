package com.sakura.spark.project.utils

import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat

/**
  * 日期工具类
  */
object DateUtils {
  /* 解析源时间格式 */
  val YYYYMMDDHHMMSS_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  /* 解析目标时间格式 */
  val TARGE_FORMAT = FastDateFormat.getInstance("yyyyMMddHHmmss")

  def getTime(time: String) = {
    YYYYMMDDHHMMSS_FORMAT.parse(time).getTime
  }

  def parseToMinute(time: String) = {
    TARGE_FORMAT.format(new Date(getTime(time)))
  }
}
