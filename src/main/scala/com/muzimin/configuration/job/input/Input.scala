package com.muzimin.configuration.job.input

import com.muzimin.input.Reader

/**
 * @author: 李煌民
 * @date: 2021-12-13 10:35
 *        ${description}
 **/
case class Input(
                  file: Option[File],
                  hive: Option[Hive],
                  jdbc: Option[JDBC],
                  obsData: Option[ObsData]
                ) extends InputConfig {
  override def getReader(name: String): Reader = {
    Seq(file, hive, jdbc, obsData).find(
      x => {
        x.isDefined
      }
    ).get.get.getReader(name)
  }
}
