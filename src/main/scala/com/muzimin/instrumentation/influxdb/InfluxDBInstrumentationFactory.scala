package com.muzimin.instrumentation.influxdb

import com.muzimin.configuration.job.instrumentation.InfluxDBConfig
import com.muzimin.instrumentation.{InstrumentationFactory, InstrumentationProvider}
import org.influxdb.{BatchOptions, InfluxDB, InfluxDBFactory}

/**
 * @author: 李煌民
 * @date: 2021-12-06 17:38
 *        ${description}
 **/
class InfluxDBInstrumentationFactory(val measurement: String, val config: InfluxDBConfig) extends InstrumentationFactory {
  val JITTER_DURATION = 500

  override def create(): InstrumentationProvider = {
    // scalastyle:off null
    var influxDB: InfluxDB = null

    config.username match {

      case Some(username) => influxDB = InfluxDBFactory.connect(config.url, username, config.password.orNull)
      case None => influxDB = InfluxDBFactory.connect(config.url)

    }

    influxDB
      .setDatabase(config.dbName)
      .enableBatch(BatchOptions.DEFAULTS.jitterDuration(JITTER_DURATION))
      .enableGzip()

    new InfluxDBInstrumentation(influxDB, measurement)
  }
}
