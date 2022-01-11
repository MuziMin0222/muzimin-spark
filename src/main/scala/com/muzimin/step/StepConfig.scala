package com.muzimin.step

import com.muzimin.configuration.step.Configuration
import com.muzimin.job.Job
import org.apache.log4j.LogManager

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

  def write(job: Job) = {
    configuration.output match {
      case Some(outputs) => {
        outputs.foreach(
          outputConfig => {

          }
        )
      }
    }
  }
}
