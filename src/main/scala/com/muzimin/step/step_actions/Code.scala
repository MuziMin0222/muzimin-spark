package com.muzimin.step.step_actions

import com.muzimin.job.RichProcessJob
import com.muzimin.step.StepAction
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.reflect.runtime.universe._
import scala.language.reflectiveCalls

/**
 * @author: 李煌民
 * @date: 2022-01-11 16:34
 *        ${description}
 **/
case class Code(
                 objectClassPath: String,
                 dataFrameName: String,
                 params: Option[Map[String, String]]
               ) extends StepAction[Unit] {

  override def run(spark: SparkSession): Unit = {
    val richProcessJob = Class.forName(objectClassPath)
      .newInstance()
      .asInstanceOf[RichProcessJob]

    richProcessJob.run(spark, dataFrameName, params)
  }
}
