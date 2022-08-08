package com.muzimin.configuration.job.output_conf

/**
 * @author: 李煌民
 * @date: 2021-11-18 00:09
 *        ${description}
 **/
case class Redis(
                  host: String,
                  port: Option[String],
                  auth: Option[String],
                  db: Option[String]
                ) {
  require(Option(host).isDefined, "Redis 数据库连接要 必须指定redis host连接")
}
