package com.muzimin.configuration.job

import com.fasterxml.jackson.annotation.JsonProperty
import com.muzimin.configuration.job.config.InputConfig
import com.muzimin.configuration.job.input.File
import com.muzimin.input.Reader

/**
 * @author: 李煌民
 * @date: 2021-12-13 10:35
 *        ${description}
 **/
case class Input(
                  file: Option[File]
                  //hive:Option[Hive]
                ) extends InputConfig {
  override def getReader(name: String): Reader = {
    Seq(file).find(
      x => {
        x.isDefined
      }
    ).get.get.getReader(name)
  }
}
