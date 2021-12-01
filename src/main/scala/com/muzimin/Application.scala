package com.muzimin

import com.muzimin.configuration.{Configuration, ConfigurationParser}
import com.muzimin.job.Job
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

/**
 * @author : 李煌民
 * @date : 2021-08-18 11:25
 *       ${description}
 **/
object Application {
  val log = LogManager.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    log.info("start muzimin - parsing configuration")
    //将传入的配置文件进行解析，形成config对象
    val config: Configuration = ConfigurationParser.parse(args)
    //根据输出类型，来创建sparkSession对象，不同的输出对应不同的SparkSession配置
    val spark: SparkSession = Job.createSparkSession(config.appName, config.output)

    try {
      val job = Job(config, Option(spark))

    } finally {
      spark.stop()
    }
  }
}
