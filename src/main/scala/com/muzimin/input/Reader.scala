package com.muzimin.input

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author : 李煌民
 * @date : 2021-09-15 20:08
 *       ${description}
 **/
trait Reader {
  val name: String

  def read(spark: SparkSession): DataFrame
}
