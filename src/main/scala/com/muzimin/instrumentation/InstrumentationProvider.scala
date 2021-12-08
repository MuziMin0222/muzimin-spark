package com.muzimin.instrumentation

import com.muzimin.configuration.job.Instrumentation
import com.muzimin.instrumentation.influxdb.{InfluxDBInstrumentationFactory, NullInstrumentationFactory}

/**
 * @author: 李煌民
 * @date: 2021-12-06 16:53
 *        ${description}
 **/
object InstrumentationProvider {
  def getInstrumentationFactory(appName: Option[String], instrumentation: Option[Instrumentation]): InstrumentationFactory = {
    instrumentation match {
      case Some(inst) => inst.influxDb match {
        case Some(influxDB) => {
          new InfluxDBInstrumentationFactory(appName.get, influxDB)
        }
        case None => new NullInstrumentationFactory()
      }
      case None => new NullInstrumentationFactory()
    }
  }
}

trait InstrumentationProvider extends Serializable {
  def count(name: String, value: Long, tags: Map[String, String] = Map(), time: Long = System.currentTimeMillis()): Unit

  def gauge(name: String, value: Long, tags: Map[String, String] = Map(), time: Long = System.currentTimeMillis()): Unit

  def close(): Unit = {}
}
