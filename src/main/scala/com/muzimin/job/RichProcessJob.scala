package com.muzimin.job

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author: 李煌民
 * @date: 2022-01-11 16:42
 *        子类可以有两作用
 *            1：完成SQL难以完成的作业
 *            2：注册UDF
 **/
trait RichProcessJob extends Serializable {
  def run(
           sparkSession: SparkSession,
           dataFrameName: String,
           params: Option[Map[String, String]]
         ): Unit
}
