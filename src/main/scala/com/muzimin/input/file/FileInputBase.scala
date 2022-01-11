package com.muzimin.input.file

import com.muzimin.utils.FileUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author: 李煌民
 * @date: 2021-12-13 11:48
 *        ${description}
 **/
trait FileInputBase {
  /**
   * 获取文件的类型，默认是txt文件
   *
   * @param format 已经传入了文件格式，就用传入的
   * @param path   文件所在的路径
   * @return
   */
  def getFormat(format: Option[String], path: String): String = {
    format match {
      case Some(f) => {
        f
      }
      case None => {
        FileUtils.getFileFormat(path)
      }
    }
  }

  /**
   * 对一些特殊的文件进行增加配置
   *
   * @param readFormat 文件的格式
   * @param options    文件的配置内容
   * @return
   */
  def getOptions(readFormat: String, options: Option[Map[String, String]]): Option[Map[String, String]] = {
    readFormat match {
      //如果读取的文件是csv，默认第一行是表头
      case "csv" => {
        Option(
          Map(
            "quote" -> "\"", //设置CSV每个字段的包裹符
            "escape" -> "\"", //设置csv文件中的字段存在双引号，进行转义处理
            "quoteAll" -> "true", //设置整个csv文件进行双引号全包裹
            "header" -> "true" //默认CSV第一行就是表头
          ) ++ options.getOrElse(Map())
        )
      }
      case _ => options
    }
  }

  /**
   * 对一些特殊的文件进行特殊处理，如csv null值填充
   *
   * @param df         传进来的DataFrame
   * @param readFormat 文件格式
   * @return
   */
  def processDF(df: DataFrame, readFormat: String): DataFrame = {
    readFormat match {
      case "csv" => {
        df.na.fill("")
      }
      case _ => df
    }
  }

  //拿到StructType，如果没传，就NONE
  def getSchemaStruct(schemaPath: Option[String]): Option[StructType] = {
    schemaPath match {
      case Some(path) => {
        Option(SchemaConverter.convert(FileUtils.readConfigurationFile(path)))
      }
      case None => None
    }
  }
}
