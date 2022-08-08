package com.muzimin.configuration.job.output

import com.fasterxml.jackson.module.scala.JsonScalaEnumeration
import com.muzimin.configuration.job.output.OutputType.OutputType

/**
 * @author: 李煌民
 * @date: 2021-12-30 18:29
 *        ${description}
 **/
case class Output(
                   name: Option[String],
                   dataFrameName: String,
                   outputOptions: Map[String, Any],
                   @JsonScalaEnumeration(classOf[OutputTypeReference]) outputType: OutputType,
                   repartition: Option[Int]
                 )
