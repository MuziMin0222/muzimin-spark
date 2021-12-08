package com.muzimin.instrumentation.influxdb

import com.muzimin.instrumentation.InstrumentationProvider

/**
 * @author: 李煌民
 * @date: 2021-12-06 18:28
 *        ${description}
 **/
class NullInstrumentation extends InstrumentationProvider{
  override def count(name: String, value: Long, tags: Map[String, String], time: Long): Unit = None

  override def gauge(name: String, value: Long, tags: Map[String, String], time: Long): Unit = None
}
