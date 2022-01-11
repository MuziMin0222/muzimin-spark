package com.muzimin.step

import org.apache.spark.sql.SparkSession

/**
 * @author: 李煌民
 * @date: 2022-01-11 15:42
 *        ${description}
 **/
trait StepAction[A] {
  def dataFrameName: String

  def run(spark: SparkSession): A
}
