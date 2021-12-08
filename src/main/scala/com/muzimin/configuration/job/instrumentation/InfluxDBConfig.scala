package com.muzimin.configuration.job.instrumentation

/**
 * @author: 李煌民
 * @date: 2021-12-06 17:34
 *        ${description}
 **/
case class InfluxDBConfig(
                           url: String,
                           username: Option[String],
                           password: Option[String],
                           dbName: String
                         )
