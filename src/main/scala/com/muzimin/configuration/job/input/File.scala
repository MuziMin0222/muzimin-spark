package com.muzimin.configuration.job.input

import com.muzimin.configuration.job.config.InputConfig
import com.muzimin.input.Reader
import com.muzimin.input.file.FileInput

/**
 * @author: 李煌民
 * @date: 2021-12-13 10:36
 *        ${description}
 **/
case class File(
                 path: String,
                 options: Option[Map[String, String]],
                 schemaPath: Option[String],
                 format: Option[String]
               ) extends InputConfig {
  override def getReader(name: String): Reader = {
    FileInput(name, path, options, schemaPath, format)
  }
}
