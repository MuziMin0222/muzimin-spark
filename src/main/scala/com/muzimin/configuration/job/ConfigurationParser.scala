package com.muzimin.configuration.job

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.muzimin.utils.{DateUtils, FileUtils}
import org.apache.log4j.{LogManager, Logger}
import scopt.OptionParser

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.io.Source

/**
 * @author : 李煌民
 * @date : 2021-09-08 11:02
 *       解析传入进来的config.yaml 或者 传入进来的json数据/文件
 * */
object ConfigurationParser {
  final val log: Logger = LogManager.getLogger(this.getClass)

  case class ConfigFileName(confFilePath: Option[String] = None,
                            dt: Option[String] = None,
                            parseDt: Option[String] = None,
                            variableFilePath: Option[String] = None,
                            mode: Option[String] = None
                           )

  private val CLIParser: OptionParser[ConfigFileName] = new scopt.OptionParser[ConfigFileName]("MuziMin") {
    head("MuziMin", "2.0")
    opt[String]('c', "conf")
      .action((x, c) => {
        c.copy(confFilePath = Option(x))
      })
      .text("Path to the job config file (YAML)")
    opt[String]('d', "dt")
      .action((x, c) => {
        c.copy(dt = Option(x))
      })
      .text("dt/t is yyyyMMdd date parse to Yaml file")
    opt[String]('p', "parseDt")
      .action((x, c) => {
        c.copy(parseDt = Option(x))
      })
      .text("parseDt/p is yyyy-MM-dd date to parse yaml file")
    opt[String]('v', "variable")
      .action((x, c) => {
        c.copy(variableFilePath = Option(x))
      })
      .text("sensitive/s is Configure sensitive data files(properties file)")
    opt[String]('m', "mode")
      .action((x, c) => {
        c.copy(mode = Option(x))
      })
      .text("executor mode, local or yarn, default yarn")

    help("help") text "use command line arguments to specify the configuration file path or content"
  }

  //解析传入的参数映射成configuration对象
  def parse(args: Array[String]): Configuration = {
    log.info("starting parsing configuration")

    CLIParser.parse(args, ConfigFileName()).flatMap { arguments =>
      arguments.confFilePath.map { fileName =>
        val dt = arguments.dt.getOrElse {
          val currentDate = LocalDate.now()
          val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
          val formattedDate = currentDate.format(formatter)
          log.info(s"-d/--dt没有传入，使用默认值${formattedDate}...")
          formattedDate
        }
        val parseDt = arguments.parseDt.getOrElse {
          val currentDate = LocalDate.now()
          val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
          val formattedDate = currentDate.format(formatter)
          log.info(s"-p/--parseDt没有传入，使用默认值${formattedDate}...")
          formattedDate
        }

        val argsMap = arguments.variableFilePath match {
          case Some(value) =>
            log.info(s"匹配上-v/--variable传进来的参数，path: ${value}，开始解析variable文件,k,v添加到MAP中...")
            Map("dt" -> dt, "parseDt" -> parseDt) ++ parseProperties(value) ++ dtList(dt)
          case None => Map("dt" -> dt, "parseDt" -> parseDt) ++ dtList(dt)
        }

        log.info(s"补充数据map：$argsMap")

        log.info(s"匹配上-c/--conf传进来的参数，path: ${fileName}，开始解析conf文件...")

        val configuration = parseConfigurationFile(FileUtils.readConfigurationFile(fileName, argsMap), FileUtils.getObjectMapperByFileName(fileName))

        val mode = arguments.mode
        log.info(s"拿到mode参数: ${mode.getOrElse("")}")
        configuration.mode = mode

        configuration
      }
    }.getOrElse {
      log.error("请传入参数...")
      throw new Exception("no argument passed to Hypers Spark")
    }
  }

  //如果变量中需要加解密，在这里进行执行
  def parseProperties(path: String): Map[String, String] = {
    val pathSource = try {
      Source.fromFile(path)
    } catch {
      case e: Exception =>
        log.error(s"${path} 没有传入, ${e.printStackTrace()}")
        return Map()
    }
    val lineItea = pathSource.getLines()
    val map = lineItea
      .filter(_.contains("="))
      .map(line => {
        val arr = line.split("=")
        (arr(0), arr(1))
      }).toMap
    //val userName = map.getOrElse("security.userName", "")
    //val random = map.getOrElse("security.random", "")

    //val pkey = AESUtils.generatePkey(userName, random)

    map.map {
      case (k, v) =>
        var decryptValue = v
        //if (v.contains("ENC_:")) {
        //  decryptValue = SecurityUtils.Decrypt(v, pkey)
        //}
        (k, decryptValue)
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

  def dtList(dt: String): Map[String, String] = {
    Map(
      "tomorrowDt" -> DateUtils.getTomorrowDt(dt),
      "yesterdayDt" -> DateUtils.getYesterdayDt(dt),
      "tomorrowParseDt" -> DateUtils.getTomorrowParseDt(dt),
      "yesterdayParseDt" -> DateUtils.getYesterdayParseDt(dt),
      "toYear" -> DateUtils.getYear(dt)
    )
  }
}
