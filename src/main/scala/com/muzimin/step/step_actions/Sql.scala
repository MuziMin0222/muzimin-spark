package com.muzimin.step.step_actions

import com.muzimin.step.StepAction
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author: 李煌民
 * @date: 2022-01-11 15:50
 *        ${description}
 **/
case class Sql(
                query: String,
                dataFrameName: String,
                showPreviewLines: Int,
                cacheInPreview: Option[Boolean],
                showQuery: Option[Boolean]
              ) extends StepAction[DataFrame] {
  val log = LogManager.getLogger(this.getClass)

  override def run(spark: SparkSession): DataFrame = {
    showQuery match {
      case Some(true) => {
        log.info(s"形成 ${dataFrameName} 的SQL语句如下: ${System.lineSeparator()} ${query}")
      }
      case _ =>
    }

    //将sql语句转为DataFrame
    val df = spark.sql(query)
    //将DataFrame转为临时表
    df.createOrReplaceTempView(dataFrameName)
    printData(df)
    df
  }

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
