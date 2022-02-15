package com.muzimin.input.jdbc

import com.muzimin.input.Reader
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author: 李煌民
 * @date: 2022-02-15 16:13
 *        ${description}
 **/
case class JDBCInput(
                      name: String,
                      connectionUrl: String,
                      user: String,
                      password: String,
                      table: String,
                      options: Option[Map[String, String]]
                    ) extends Reader {
  override def read(spark: SparkSession): DataFrame = {
    val baseDBOptions = Map(
      "url" -> connectionUrl,
      "user" -> user,
      "password" -> password,
      "dbTable" -> table)

    val DBOptions = baseDBOptions ++ options.getOrElse(Map())

    val dbTable = spark.read.format("jdbc").options(DBOptions)
    dbTable.load()
  }
}
