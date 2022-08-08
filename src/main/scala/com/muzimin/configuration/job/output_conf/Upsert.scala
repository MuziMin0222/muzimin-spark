package com.muzimin.configuration.job.output_conf

/**
 * @author: 李煌民
 * @date: 2022-03-16 14:17
 *        ${description}
 **/
case class Upsert(
                   connectionUrl: String,
                   user: String,
                   password: String,
                   driver: String,
                   duplicateIncs: Option[String],
                   format: Option[String],
                   saveMode: Option[String],
                   useSSL: Option[String],
                   showSql: Option[String]
                 )
