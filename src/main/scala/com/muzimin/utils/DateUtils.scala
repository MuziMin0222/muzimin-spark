package com.muzimin.utils

import java.time.LocalDate
import java.time.format.DateTimeFormatter

/**
 * @author: 李煌民
 * @date: 2024-04-30 14:09
 *        ${description}
 * */
object DateUtils {
  private final val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
  private final val formaterParse = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private final val formaterYear = DateTimeFormatter.ofPattern("yyyy")

  def getYesterdayDt(dt: String): String = {
    val date = LocalDate.parse(dt, formatter)

    val previousDate = date.minusDays(1)
    previousDate.format(formatter)
  }

  def getYesterdayParseDt(dt: String): String = {
    val date = LocalDate.parse(dt, formatter)

    val previousDate = date.minusDays(1)
    previousDate.format(formaterParse)
  }

  def getTomorrowDt(dt: String): String = {
    val date = LocalDate.parse(dt, formatter)

    val previousDate = date.plusDays(1)
    previousDate.format(formatter)
  }

  def getTomorrowParseDt(dt: String): String = {
    val date = LocalDate.parse(dt, formatter)

    val previousDate = date.plusDays(1)
    previousDate.format(formaterParse)
  }

  def getYear(dt: String): String = {
    val date = LocalDate.parse(dt, formatter)

    val previousDate = date.plusDays(1)
    previousDate.format(formaterYear)
  }
}
