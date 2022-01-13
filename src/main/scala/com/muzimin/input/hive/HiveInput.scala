package com.muzimin.input.hive

import com.muzimin.input.Reader
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author: 李煌民
 * @date: 2022-01-13 16:59
 *        ${description}
 **/
case class HiveInput(
                      name: String,
                      dbName: Option[String],
                      tbName: String,
                      columns: Option[String],
                      condition: Option[String]
                    ) extends Reader {

  override def read(spark: SparkSession): DataFrame = {
    spark.sql(createSql)
  }

  private def createSql: String = {
    val db_name = dbName match {
      case Some(dbName) => {
        dbName
      }
      case None => "default"
    }

    val cols = columns match {
      case Some(columns) => {
        columns
      }
      case None => {
        "*"
      }
    }

    val cdt = condition match {
      case Some(condition) => {
        condition
      }
      case None => {
        "1 = 1"
      }
    }

    s"select ${cols} from $db_name.$tbName where $cdt"
  }
}
