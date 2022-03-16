package com.muzimin.output.wirtes.jdbc

import com.muzimin.configuration.job.output.Upsert
import com.muzimin.output.Writer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.jdbc2.JDBCSaveMode

/**
 * @author: 李煌民
 * @date: 2022-03-16 15:24
 *        ${description}
 **/
class UpsertOutputWriter(props: Map[String, Any], upsertConf: Option[Upsert]) extends Writer {
  /**
   * 用户自定义将DataFrame写出到其他地方
   *
   * @param dataFrame DataFrame结果集
   */
  override def write(dataFrame: DataFrame): Unit = {
    val upsert: Upsert = upsertConf.get

    val format = upsert.format.getOrElse("org.apache.spark.sql.execution.datasources.jdbc2")
    log.info("format:" + format)

    val update = JDBCSaveMode.Update.toString
    val saveMode: String = upsert.saveMode.getOrElse(update)
    log.info("saveMode:" + saveMode)

    val driver = upsert.driver
    log.info("driver:" + driver)

    val url = upsert.connectionUrl
    log.info("url:" + url)

    val user = upsert.user
    log.info("user:" + user)

    val passwd = upsert.password
    log.info("passwd:" + passwd)

    val dbTable = props("dbTable").toString
    log.info("dbTable:" + dbTable)

    val duplicate: String = upsert.duplicateIncs
    log.info("duplicate:" + duplicate)

    val showSql = upsert.showSql.getOrElse("true")
    log.info("showSql" + showSql)

    dataFrame
      .write
      .format(format)
      .options(
        Map(
          "savemode" -> saveMode,
          "driver" -> driver,
          "url" -> url,
          "user" -> user,
          "password" -> passwd,
          "dbtable" -> dbTable,
          "duplicateIncs" -> duplicate,
          "showSql" -> showSql
        )
      ).save()
  }
}
