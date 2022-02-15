package com.muzimin.output.wirtes.jdbc

import java.sql.DriverManager
import java.util.Properties

import com.muzimin.configuration.job.output.JDBC
import com.muzimin.output.Writer
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
 * @author: 李煌民
 * @date: 2022-02-15 16:19
 *        ${description}
 **/
class JdbcOutputWriter(props: Map[String, Any], jdbcConf: Option[JDBC]) extends Writer {

  case class JDBCOutputProperties(saveMode: SaveMode, dbTable: String)

  val dbOptions = JDBCOutputProperties(SaveMode.valueOf(props("saveMode").toString), props("dbTable").toString)

  /**
   * 用户自定义将DataFrame写出到其他地方
   *
   * @param dataFrame DataFrame结果集
   */
  override def write(dataFrame: DataFrame): Unit = {
    jdbcConf match {
      case Some(jdbcConf) =>
        val connectionProperties = new Properties()

        connectionProperties.put("user", jdbcConf.user)
        connectionProperties.put("password", jdbcConf.password)
        connectionProperties.put("driver", jdbcConf.driver)

        if (jdbcConf.truncate.isDefined) {
          connectionProperties.put("truncate", jdbcConf.truncate.get)
        }
        if (jdbcConf.cascadeTruncate.isDefined) {
          connectionProperties.put("cascadeTruncate", jdbcConf.cascadeTruncate.get)
        }
        if (jdbcConf.createTableColumnTypes.isDefined) {
          connectionProperties.put("createTableColumnTypes", jdbcConf.createTableColumnTypes.get)
        }
        if (jdbcConf.createTableOptions.isDefined) {
          connectionProperties.put("createTableOptions", jdbcConf.createTableOptions.get)
        }
        if (jdbcConf.sessionInitStatement.isDefined) {
          connectionProperties.put("sessionInitStatement", jdbcConf.sessionInitStatement.get)
        }
        val df = dataFrame
        df.write.format(jdbcConf.driver)
          .mode(dbOptions.saveMode)
          .jdbc(jdbcConf.connectionUrl, dbOptions.dbTable, connectionProperties)

        props.get("postQuery") match {
          case Some(query) =>
            val conn = DriverManager.getConnection(jdbcConf.connectionUrl, jdbcConf.user, jdbcConf.password)
            val stmt = conn.prepareStatement(query.toString)
            stmt.execute()
            stmt.close()
            conn.close()
          case _ =>
        }

      case None =>
    }
  }
}
