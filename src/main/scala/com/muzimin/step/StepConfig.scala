package com.muzimin.step

import com.muzimin.configuration.step.{Configuration, Output}
import com.muzimin.job.Job
import com.muzimin.output.WriteFactory
import org.apache.log4j.LogManager
import org.apache.spark.sql.DataFrame

/**
 * @author: 李煌民
 * @date: 2022-01-04 22:13
 *        ${description}
 **/
case class StepConfig(
                       configuration: Configuration,
                       stepFileName: String
                     ) {
  val log = LogManager.getLogger(this.getClass)

  log.info("操作步骤执行文件的配置项如下：" + configuration.toString)

  //执行计算步骤
  def transform(job: Job): Unit = {
    configuration.steps.foreach(
      stepConfig => {
        val stepAction: StepAction[_] = StepActionFactory.getStepAction(
          stepConfig,
          job.config.showPreviewLines.get,
          job.config.cacheOnPreview,
          job.config.showQuery
        )

        try {
          log.info(s"开始执行计算步骤：${stepConfig.dataFrameName}")
          stepAction.run(job.spark)
        } catch {
          case e: Exception => {
            throw new Exception("任务执行失败" + e.printStackTrace())
          }
        }
      }
    )
  }

  /**
   * repartition 可以是在output中定义，也可以在output.outputOptions中定义
   *
   * @param outputConfig
   * @param dataFrame
   */
  private def repartition(outputConfig: Output, dataFrame: DataFrame): DataFrame = {
    val map = Option(outputConfig.outputOptions).getOrElse(Map())

    val repartitionNum: Option[Int] = map.get("repartition").asInstanceOf[Option[Int]]

    outputConfig.repartition.orElse(repartitionNum) match {
      case Some(x) => {
        dataFrame.repartition(x)
      }
      case _ => dataFrame
    }
  }

  def write(job: Job): Unit = {
    configuration.output match {
      case Some(outputs) => {
        outputs.foreach(
          outputConfig => {
            //根据配置的不同创建不同的writer
            val writer = WriteFactory.getWriter(outputConfig, job.config, job)

            //该DataFrameName是Step中的中确认的DataFrameName
            val dataFrameName = outputConfig.dataFrameName

            //将临时表转为DataFrame
            val dataFrame = job.spark.table(dataFrameName)
            val repartitionDF = repartition(outputConfig, dataFrame)

            log.info(s"开始将${dataFrameName}的数据写入到${outputConfig.outputType}中")
            //try {
            writer.write(repartitionDF)
            //} catch {
            //  case e:Exception => {
            //    throw new Exception(s"${dataFrameName}的数据写入到${outputConfig.outputType}失败，请检查步骤文件是否正确")
            //  }
            //}
          }
        )
      }
      case None =>
    }
  }
}
