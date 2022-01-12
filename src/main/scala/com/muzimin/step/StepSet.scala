package com.muzimin.step

import com.muzimin.configuration.step.{Configuration, ConfigurationParser}
import com.muzimin.job.Job
import org.apache.log4j.LogManager

/**
 * @author: 李煌民
 * @date: 2021-12-30 15:25
 *        ${description}
 **/
class StepSet(stepPath: String, write: Boolean = true) {
  val log = LogManager.getLogger(this.getClass)

  //为了初始化StepSet就进行解析配置文件
  val stepConfSeq: Seq[StepConfig] = parseStep

  def parseStep: Seq[StepConfig] = {
    log.info("开始解析step配置文件")

    Seq(ConfigurationParser.parse(stepPath))
  }

  def run(job: Job):Unit = {
    stepConfSeq.foreach(
      step => {
        val startTime = System.currentTimeMillis()

        step.transform(job)
        if (write) {
          step.write(job)
        }

        val endTime = System.currentTimeMillis()
        log.info(step.stepFileName +  " 任务执行的时间：" + (endTime - startTime) + "毫秒")
      }
    )
  }
}
