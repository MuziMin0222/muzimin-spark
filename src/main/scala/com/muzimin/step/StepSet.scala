package com.muzimin.step

import com.muzimin.configuration.job.ConfigurationParser
import com.muzimin.configuration.job.step.Step
import com.muzimin.job.Job
import org.slf4j.LoggerFactory

/**
 * @author: 李煌民
 * @date: 2021-12-30 15:25
 *        ${description}
 **/
class StepSet(step: Step, job: Job) {
  val log = LoggerFactory.getLogger(this.getClass)

  //为了初始化StepSet就进行解析配置文件
  val stepConf: StepConfig = parseStep

  def parseStep: StepConfig = {
    log.info(s"开始执行任务，得到的结果注册为： ${step.dataFrameName}")

    StepConfig(step, job)
  }

  def run(job: Job): Unit = {

    val startTime = System.currentTimeMillis()

    stepConf.transform(job)
    stepConf.write(job, step.dataFrameName)

    val endTime = System.currentTimeMillis()
    log.info(step.dataFrameName + " 任务执行的时间：" + (endTime - startTime) + "毫秒")
  }
}
