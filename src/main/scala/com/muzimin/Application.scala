package com.muzimin

import com.muzimin.configuration.job.{Configuration, ConfigurationParser}
import com.muzimin.job.Job
import com.muzimin.step.StepSet
import org.slf4j.LoggerFactory
import org.apache.spark.sql.SparkSession

/**
 * @author : 李煌民
 * @date : 2021-08-18 11:25
 *       ${description}
 **/
object Application {
  val log = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    log.info("start muzimin - parsing configuration")
    //将传入的配置文件进行解析，形成config对象
    val config: Configuration = ConfigurationParser.parse(args)
    log.info("配置文件内容如下：" + config)
    //根据输出类型，来创建sparkSession对象，不同的输出对应不同的SparkSession配置
    val spark: SparkSession = Job.createSparkSession(config)

    try {
      val job = Job(config, Option(spark))

      executeBatchJob(job)

    } finally {
      spark.stop()
    }
  }

  def executeBatchJob(job: Job): Unit = {
    job.config.steps match {
      case Some(steps) => {
        steps.foreach(
          step => {
            val stepSet = new StepSet(step, job)
            stepSet.run(job)
          }
        )
      }
      case None => {
        log.error("没有执行步骤的定义，退出程序")
      }
    }
  }
}
