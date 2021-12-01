package com.muzimin.job

import com.muzimin.configuration.Configuration
import com.muzimin.configuration.job.Output
import com.muzimin.output.wirtes.hive.HiveOutputWriter
import com.muzimin.output.wirtes.redis.RedisOutputWriter
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession


/**
 * @author: 李煌民
 * @date: 2021-11-17 23:22
 *        ${description}
 **/
object Job {
  private val log = LogManager.getLogger(this.getClass)

  def createSparkSession(appName: Option[String], output: Option[Output]): SparkSession = {
    val sparkBuilder: SparkSession.Builder = SparkSession.builder().appName(appName.get)

    output match {
      case Some(out) => {
        out.hive match {
          case Some(hive) => {
            log.info("数据输出到Hive中，配置hive所需参数")
            HiveOutputWriter.addConfToSparkSession(sparkBuilder, hive)
          }
          case None => {
            log.warn("hive对象不正确，不开启spark外接hive元数据")
          }
        }
      }
      case None => {
        log.info("不需要独立添加sparkSession的配置项")
      }
    }

    sparkBuilder.getOrCreate()
  }
}

case class Job(config: Configuration, sparkSession: Option[SparkSession] = None) {
  private val log = LogManager.getLogger(this.getClass)

  //sparkSession 定义
  val spark = sparkSession match {
    case Some(ss) => ss
    case _ => {
      Job.createSparkSession(config.appName, config.output)
    }
  }
  val sc = spark.sparkContext
}
