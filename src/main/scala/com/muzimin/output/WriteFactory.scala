package com.muzimin.output

import com.muzimin.configuration.job.{Configuration, output}
import com.muzimin.configuration.step.Output
import com.muzimin.configuration.step.output.OutputType
import com.muzimin.job.Job
import com.muzimin.output.wirtes.file.FileOutputWriter
import com.muzimin.output.wirtes.hive.HiveOutputWriter


/**
 * @author: 李煌民
 * @date: 2022-01-12 12:01
 *        ${description}
 **/
object WriteFactory {
  def getWriter(outputConfig: Output, configuration: Configuration, job: Job): Writer = {
    val output = outputConfig.name match {
      case Some(name) => {
        val value: com.muzimin.configuration.job.output.Output = configuration.outputs.get(name)
        value
      }
      case None => {
        val output: com.muzimin.configuration.job.output.Output = configuration.output.getOrElse(com.muzimin.configuration.job.output.Output())
        output
      }
    }

    val stepOutputOptions: Map[String, Any] = outputConfig.outputOptions

    val writer = outputConfig.outputType match {
      case OutputType.File => {
        new FileOutputWriter(stepOutputOptions, output.file)
      }
      case OutputType.Hive => {
        new HiveOutputWriter(outputConfig: Output, output.hive, job.spark)
      }
      case _ => {
        throw new Exception(s"不支持的写出操作==> ${outputConfig.outputType}")
      }
    }

    writer
  }
}
