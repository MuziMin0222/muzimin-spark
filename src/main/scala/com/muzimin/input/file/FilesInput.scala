package com.muzimin.input.file

import com.muzimin.input.Reader
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author: 李煌民
 * @date: 2021-12-15 14:51
 *        ${description}
 **/
case class FilesInput(
                       name: String,
                       paths: Seq[String],
                       options: Option[Map[String, String]],
                       schemaPath: Option[String],
                       format: Option[String]
                     ) extends Reader with FileInputBase {
  override def read(spark: SparkSession): DataFrame = {
    //获取文件类型
    val readFormat = getFormat(format, paths.head)
    val reader = spark.read.format(readFormat)

    val readOptions = getOptions(readFormat, options)
    val schema = getSchemaStruct(schemaPath)

    readOptions match {
      case Some(opts) => reader.options(opts)
      case None =>
    }

    schema match {
      case Some(schemaStruct) => reader.schema(schemaStruct)
      case None =>
    }

    val df = reader.load(paths: _*)

    processDF(df, readFormat)
  }
}
