package com.muzimin.step

import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author: 李煌民
 * @date: 2022-01-11 15:42
 *        ${description}
 **/
trait StepAction[A] {
  val log = LogManager.getLogger(this.getClass)

  def dataFrameName: String

  def showPreviewLines: Int

  def cacheInPreview: Option[Boolean]

  def run(spark: SparkSession): A

  def printData(df: DataFrame): Unit = {
    if (showPreviewLines > 0) {
      cacheInPreview match {
        case Some(true) => {
          log.info(s"缓存 ${dataFrameName} DataFrame")
          df.persist()
        }
        case _ =>
      }

      log.info(s"开始展示${dataFrameName}的数据")
      df.show(showPreviewLines, truncate = false)
    }
  }
}
