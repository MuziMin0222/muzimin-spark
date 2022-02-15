package com.muzimin.configuration.step

import java.io.{File, FileNotFoundException}

import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.muzimin.step.StepConfig
import com.muzimin.utils.FileUtils
import org.apache.log4j.LogManager

/**
 * @author: 李煌民
 * @date: 2021-12-30 17:17
 *        解析step.yaml文件
 **/
object ConfigurationParser {
  val log = LogManager.getLogger(this.getClass)

  def parse(path: String): StepConfig = {
    val fileName = path.substring(path.lastIndexOf(File.separator) + 1)
    log.info(s"开始实例化步骤文件 $fileName ,该文件的内容是: ${System.lineSeparator()} ${FileUtils.readConfigurationFile(path)}")

    try {
      val stepConfig = parseFile(path)
      log.info("stepConfig配置文件内容如下 : " + stepConfig)
      StepConfig(stepConfig, fileName)
    } catch {
      case e: FileNotFoundException => {
        throw new FileNotFoundException("配置的step文件找不到，请检查配置路径")
      }
      case e: Exception => {
        throw new Exception("文件解析异常，请检查step文件是否是标准yaml/json文件")
      }
    }
  }

  private def parseFile(fileName: String): Configuration = {
    FileUtils.getObjectMapperByExtension(fileName) match {
      case Some(mapper) => {
        mapper.registerModule(DefaultScalaModule)
        mapper.readValue(FileUtils.readConfigurationFile(fileName), classOf[Configuration])
      }
      case None => {
        throw new Exception(s"step文件有误，请核对${fileName} step文件 ")
      }
    }
  }
}
