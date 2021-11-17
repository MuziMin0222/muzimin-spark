package com.muzimin.job

import com.muzimin.bean.job.Output
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession


/**
 * @author: 李煌民
 * @date: 2021-11-17 23:22
 *        ${description}
 **/
object Job {
  lazy val log = LogManager.getLogger(this.getClass)

  def createSparkSession(appName: Option[String], output: Option[Output]): SparkSession = {
    val sparkBuilder: SparkSession.Builder = SparkSession.builder().appName(appName.get)

    output match {
      case Some(out) => {
        out.redis match {
          case Some(redis) => {
            log.info("输出是redis，独立配置sparkSession参数")
          }
          case None => {
            log.warn("匹配上redis，但redis对象不正确，不进行sparkSession的独立配置")
          }
        }
      }
      case None => {
        log.info("不需要独立添加sparkSession的配置项")
      }
    }

    null
  }
}
