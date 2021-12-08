package com.muzimin.instrumentation.influxdb

import java.util.concurrent.TimeUnit

import com.muzimin.instrumentation.InstrumentationProvider
import org.influxdb.InfluxDB
import org.influxdb.dto.Point
import scala.collection.JavaConverters.mapAsJavaMapConverter

/**
 * @author: 李煌民
 * @date: 2021-12-06 18:23
 *        ${description}
 **/
class InfluxDBInstrumentation(val influxDB: InfluxDB, val measurement: String) extends InstrumentationProvider {
  override def count(name: String, value: Long, tags: Map[String, String], time: Long): Unit = {
    writeToInflux(time, name, value, tags)
  }

  override def gauge(name: String, value: Long, tags: Map[String, String], time: Long): Unit = {
    writeToInflux(time, name, value, tags)
  }

  private def writeToInflux(time: Long, name: String, value: Long, tags: Map[String, String] = Map()): Unit = {
    influxDB.write(Point.measurement(measurement)
      .time(time, TimeUnit.MILLISECONDS)
      .addField(name, value)
      .tag(tags.asJava)
      .build())
  }

  override def close(): Unit = {
    influxDB.close()
  }
}
