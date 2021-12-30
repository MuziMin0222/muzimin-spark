package com.muzimin.step

import org.apache.log4j.LogManager

/**
 * @author: 李煌民
 * @date: 2021-12-30 15:25
 *        ${description}
 **/
class StepSet(stepPath: String, write: Boolean = true) {
  val log = LogManager.getLogger(this.getClass)

  def parseStep = {
    log.info("开始解析step配置文件")

  }
}
