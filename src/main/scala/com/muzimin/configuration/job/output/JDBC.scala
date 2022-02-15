package com.muzimin.configuration.job.output

/**
 * @author: 李煌民
 * @date: 2022-02-15 16:20
 *        ${description}
 **/
case class JDBC(
                 connectionUrl: String,
                 user: String,
                 password: String,
                 driver: String,
                 sessionInitStatement: Option[String],
                 truncate: Option[String],
                 cascadeTruncate: Option[String],
                 createTableOptions: Option[String],
                 createTableColumnTypes: Option[String]
               ) {
  require(Option(connectionUrl).isDefined, "JDBC connection: connection url 是输出配置的必选项")
  require(Option(user).isDefined, "JDBC connection: user 是输出配置的必选项")
  require(Option(password).isDefined, "JDBC connection: password 是输出配置的必选项")
  require(Option(driver).isDefined, "JDBC connection: driver 是输出配置的必选项")
}
