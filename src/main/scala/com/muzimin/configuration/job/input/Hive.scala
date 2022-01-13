package com.muzimin.configuration.job.input

import com.muzimin.input.Reader
import com.muzimin.input.hive.HiveInput

/**
 * @author: 李煌民
 * @date: 2022-01-13 16:42
 *        ${description}
 **/
case class Hive(
                 dbName: Option[String],
                 tbName: String,
                 columns: Option[String],
                 condition: Option[String]
               ) extends InputConfig {
  override def getReader(name: String): Reader = {
    HiveInput(name, dbName, tbName, columns, condition)
  }
}
