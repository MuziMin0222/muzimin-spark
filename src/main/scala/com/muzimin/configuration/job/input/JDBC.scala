package com.muzimin.configuration.job.input

import com.muzimin.input.Reader
import com.muzimin.input.jdbc.JDBCInput

/**
 * @author: 李煌民
 * @date: 2022-02-15 16:03
 *        ${description}
 **/
case class JDBC(
                 connectionUrl: String,
                 user: String,
                 password: String,
                 table: String,
                 options: Option[Map[String, String]]
               ) extends InputConfig {
  require(Option(connectionUrl).isDefined, "JDBC connection: connection url 是输入配置的必选项")
  require(Option(user).isDefined, "JDBC connection: user 是输入配置的必选项")
  require(Option(password).isDefined, "JDBC connection: password 是输入配置的必选项")
  require(Option(table).isDefined, "JDBC connection: table 是输入配置的必选项")

  override def getReader(name: String): Reader = {
    JDBCInput(name,connectionUrl, user, password, table, options)
  }
}
