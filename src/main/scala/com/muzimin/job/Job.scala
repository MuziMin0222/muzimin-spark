package com.muzimin.job

import com.muzimin.configuration.job.Configuration
import com.muzimin.configuration.job.output_conf.OutputConf
import com.muzimin.input.Reader
import com.muzimin.output.wirtes.hive.HiveOutputWriter
import com.muzimin.output.wirtes.redis.RedisOutputWriter
import org.slf4j.LoggerFactory
import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd}
import org.apache.spark.sql.SparkSession


/**
 * @author: 李煌民
 * @date: 2021-11-17 23:22
 *        ${description}
 **/
object Job {
  private val log = LoggerFactory.getLogger(this.getClass)

  def createSparkSession(config: Configuration): SparkSession = {
    //在yarn cluster中设置的appName不生效，优先读取命令行中--name所带的名称，没有该参数，则使用启动类的全路径
    var sparkBuilder: SparkSession.Builder = SparkSession.builder().appName(config.appName.get)
    config.mode match {
      case Some(value) => value match {
        case "local" =>
          log.info("MUZIMIN SPARK LOG | 在本地环境进行执行")
          sparkBuilder = sparkBuilder.master("local[*]")
      }
      case None =>
    }

    config.outputConf match {
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
  private val log = LoggerFactory.getLogger(this.getClass)

  //sparkSession 定义
  val spark = sparkSession match {
    case Some(ss) => ss
    case _ => {
      Job.createSparkSession(config)
    }
  }
  val sc = spark.sparkContext

  //配置日志级别
  setSparkLogLevel(config.logLevel, sc)
  //将变量设置在sql中
  registerVariable(config.variables, spark)
  //将配置文件中配置的Input，转为DataFrame，并注册为临时表
  registerDataFrames(config.getReaders, spark)

  //如果配置了catalog，在spark环境中配置一下，目前只有Database一个配置项
  config.catalog match {
    case Some(catalog) => {
      catalog.database match {
        case Some(database) => {
          spark.sql(s"CREATE DATABASE IF NOT EXISTS $database")
          spark.catalog.setCurrentDatabase(database)
        }
        case None =>
      }
    }
    case None =>
  }

  //设置spark log的日志级别
  private def setSparkLogLevel(logLevel: Option[String], sc: SparkContext): Unit = {
    logLevel match {
      case Some(logLevel) => {
        sc.setLogLevel(logLevel)
      }
      case None =>
    }
  }

  //如果配置了参数，比如动态分区，在该方法中进行设定
  private def registerVariable(variable: Option[Map[String, String]], spark: SparkSession): Unit = {
    variable.getOrElse(Map())
      .foreach({
        case (k, v) => {
          spark.sql(s"set $k = '$v'")
        }
      })
  }

  //将配置文件中的input封装为DataFrame，并注册为临时表
  private def registerDataFrames(inputs: Seq[Reader], spark: SparkSession): Unit = {
    if (inputs.nonEmpty) {
      inputs.foreach(
        input => {
          log.info(s"开始将配置的input: ${input.name}选项注册为DataFrame,并创建临时表，表名为：${input.name}")
          val df = input.read(spark)

          df.createOrReplaceTempView(input.name)
        }
      )
    }
  }
}
