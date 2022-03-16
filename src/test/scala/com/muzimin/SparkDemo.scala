package com.muzimin

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.jdbc2.JDBCSaveMode

/**
 * @author: 李煌民
 * @date: 2022-03-16 16:03
 *        ${description}
 **/
case class a(id: String, name: String, pv: Int, uv: Int)

object SparkDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.makeRDD(Seq(a("1","lhm",12,43))).toDF
      .write
      .format("org.apache.spark.sql.execution.datasources.jdbc2")
      .options(
        Map(
          "savemode" -> JDBCSaveMode.Update.toString,
          "driver" -> "com.mysql.jdbc.Driver",
          "url" -> "jdbc:mysql://localhost:3306/test",
          "user" -> "root",
          "password" -> "Lhm18779700731!",
          "dbtable" -> "tb_b",
          "useSSL" -> "false",
          "duplicateIncs" -> "pv,uv",
          "showSql" -> "true"
        )
      ).save()

    spark.close()
  }
}
