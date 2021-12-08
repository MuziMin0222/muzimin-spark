package com.muzimin.configuration.job

import com.muzimin.configuration.job.instrumentation.InfluxDBConfig

/**
 * @author: 李煌民
 * @date: 2021-12-06 17:22
 *        ${description}
 **/
case class Instrumentation(influxDb: Option[InfluxDBConfig])
