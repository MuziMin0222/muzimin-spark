package com.muzimin.configuration.step

import java.io.File

import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.muzimin.utils.FileUtils
import org.apache.log4j.LogManager

/**
 * @author: 李煌民
 * @date: 2021-12-30 17:17
 *        解析step.yaml文件
 **/
object ConfigurationParser {
  val log = LogManager.getLogger(this.getClass)

  def parse(path: String) = {
    val fileName = path.substring(path.lastIndexOf(File.separator) + 1)
    log.info(s"开始实例化步骤文件 $fileName")
  }

  private def parseFile(fileName: String): Configuration = {
    FileUtils.getObjectMapperByExtension(fileName) match {
      case Some(mapper) => {
        mapper.registerModule(DefaultScalaModule)
        mapper.readValue(FileUtils.readConfigurationFile(fileName), classOf[Configuration])
      }
      case None => {
        throw new Exception("step文件有误，请核对step文件")
      }
    }
    null
  }
}
