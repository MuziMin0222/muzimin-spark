package com.muzimin.configuration.job.output

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.module.scala.JsonScalaEnumeration

/**
 * @author: 李煌民
 * @date: 2022-01-12 15:00
 *        输出类型的枚举类
 **/
object OutputType extends Enumeration {
  type OutputType = Value

  val Parquet, CSV, JSON, File, Hive, JDBC, Upsert = Value
}