package com.muzimin.configuration.job

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.muzimin.utils.FileUtils
import org.apache.log4j.{LogManager, Logger}
import scopt.OptionParser

/**
 * @author : 李煌民
 * @date : 2021-09-08 11:02
 *       解析传入进来的config.yaml 或者 传入进来的json数据/文件
 * */
object ConfigurationParser {
  final val log: Logger = LogManager.getLogger(this.getClass)

  case class ConfigFileName(confFilePath: Option[String] = None, dt: Option[String] = None)

  private val CLIParser: OptionParser[ConfigFileName] = new scopt.OptionParser[ConfigFileName]("MuziMin") {
    head("MuziMin", "2.0")
    opt[String]('c', "conf")
      .action((x, c) => {
        c.copy(confFilePath = Option(x))
      })
      .text("Path to the job config file (YAML/JSON)")
    opt[String]('d', "dt")
      .action((x, c) => {
        c.copy(dt = Option(x))
      })
      .text("dt/t is yyyyMMdd date parse to Yaml file")

    help("help") text "use command line arguments to specify the configuration file path or content"
  }

  //解析传入的参数映射成configuration对象
  def parse(args: Array[String]): Configuration = {
    log.info("starting parsing configuration")

    CLIParser.parse(args, ConfigFileName()) match {
      case Some(arguments) => {
        arguments.confFilePath match {
          //将-c 参数中的数据映射到此处
          case Some(fileName) => {
            log.info("匹配上-c/--conf传进来的参数，开始解析conf文件...")
            parseConfigurationFile(FileUtils.readConfigurationFile(fileName), FileUtils.getObjectMapperByFileName(fileName))
          }
          case None => {
            log.error("请传入正确的参数，-c/--conf传入conf文件")
            throw new Exception("Failed to Parse Config file(没有匹配上文件)...")
          }
        }
      }
      case None => {
        log.error("请传入参数...")
        throw new Exception("no argument passed to MuziMin Spark")
      }
    }
  }

  //解析JSON最简单的方法  可以将对象转为我们需要的类型，使用object强制转换会出问题
  def parseConfigurationFile(job: String, mapper: Option[ObjectMapper]): Configuration = {
    mapper match {
      case Some(mapper) => {
        mapper.registerModule(DefaultScalaModule)
        mapper.readValue(job, classOf[Configuration])
      }
      case None => {
        throw new Exception("File extension should be json or yaml")
      }
    }
  }
}
