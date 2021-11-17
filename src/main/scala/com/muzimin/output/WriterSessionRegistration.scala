package com.muzimin.output

import org.apache.spark.sql.SparkSession

/**
 * @author: 李煌民
 * @date: 2021-11-18 00:05
 *        ${description}
 **/
trait WriterSessionRegistration {
  def addToSparkSession(spark: SparkSession): Unit = {}
}
