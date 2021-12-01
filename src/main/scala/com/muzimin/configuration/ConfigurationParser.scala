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

  case class ConfigFileName(jobFilePath: Option[String] = None, confFilePath: Option[String] = None)

  private val CLIParser: OptionParser[ConfigFileName] = new scopt.OptionParser[ConfigFileName]("MuziMin") {
    head("MuziMin", "1.0")
    opt[String]('j', "job")
      .action((x, c) => {
        c.copy(jobFilePath = Option(x))
      })
      .text("Job configuration JSON")
    opt[String]('c', "conf")
      .action((x, c) => {
        c.copy(confFilePath = Option(x))
      })
      .text("Path to the job config file (YAML/JSON)")

    help("help") text "use command line arguments to specify the configuration file path or content"
  }

  //解析传入的参数映射成configuration对象
  def parse(args: Array[String]): Configuration = {
    log.info("starting parsing configuration")

    CLIParser.parse(args, ConfigFileName()) match {
      case Some(arguments) => {
        arguments.jobFilePath match {
          //将-j参数中的数据映射到此处
          case Some(job) => {
            log.info("匹配上-j/--job传进来的参数,开始解析json数据...")
            parseConfigurationFile(job, FileUtils.getObjectMapperByExtension("json"))
          }
          case None => arguments.confFilePath match {
            //将-c 参数中的数据映射到此处
            case Some(fileName) => {
              log.info("匹配上-c/--conf传进来的参数，开始解析conf文件...")
              parseConfigurationFile(FileUtils.readConfigurationFile(fileName), FileUtils.getObjectMapperByFileName(fileName))
            }
            case None => {
              log.error("请传入正确的参数，-c/--conf传入conf文件， -j/-job 传入json数据")
              throw new Exception("Failed to Parse Config file(没有匹配上文件)...")
            }
          }
        }
      }
      case None => {
        log.error("请传入参数...")
        throw new Exception("no argument passed to MuziMin")
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
