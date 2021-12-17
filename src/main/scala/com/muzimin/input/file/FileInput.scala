package com.muzimin.input.file

import com.muzimin.input.Reader
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author: 李煌民
 * @date: 2021-12-15 14:48
 *        ${description}
 **/
case class FileInput(
                      name: String,
                      path: String,
                      options: Option[Map[String, String]],
                      schemaPath: Option[String],
                      format: Option[String]
                    ) extends Reader {
  override def read(spark: SparkSession): DataFrame = {
    FilesInput(
      name,
      Seq(path),
      options,
      schemaPath,
      format
    ).read(spark)
  }
}
