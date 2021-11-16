package com.muzimin.configuration

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.muzimin.utils.FileUtils
import org.apache.log4j.{LogManager, Logger}
import scopt.OptionParser

/**
 * @author : 李煌民
 * @date : 2021-09-08 11:02
 *       ${description}
 **/
object ConfigurationParser {
  final val log: Logger = LogManager.getLogger(this.getClass)

  case class ConfigFileName(job: Option[String] = None, fileName: Option[String] = None)

  private val CLIParser: OptionParser[ConfigFileName] = new scopt.OptionParser[ConfigFileName]("MuziMin") {
    head("MuziMin", "1.0")
    opt[String]('j', "job")
      .action((x, c) => {
        c.copy(job = Option(x))
      })
      .text("Job configuration JSON")
    opt[String]('c', "conf")
      .action((x, c) => {
        c.copy(fileName = Option(x))
      })
      .text("Path to the job config file (YAML/JSON)")

    help("help") text "use command line arguments to specify the configuration file path or content"
  }

  //解析传入的参数映射成configuration对象
  def parse(args: Array[String]) = {
    log.info("starting parsing configuration")

    CLIParser.parse(args, ConfigFileName()) match {
      case Some(arguments) => {
        arguments.job match {
          //将-j参数中的数据映射到此处
          case Some(job) => {
            parseConfigurationFile(job, FileUtils.getObjectMapperByExtension("json"))
          }
          case None => arguments.fileName match {
            //将-c 参数中的数据映射到此处
            case Some(fileName) => {
              println("fileName: " + fileName)
            }
            case None => {
              throw new Exception("Failed to Parse Config file")
            }
          }
        }
      }
      case None => throw new Exception("no argument passed to MuziMin")
    }
  }

  //ObjectMapper  可以将对象转为我们需要的类型，使用object强制转换会出问题
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
