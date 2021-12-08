package com.muzimin.instrumentation.influxdb

import com.muzimin.instrumentation.{InstrumentationFactory, InstrumentationProvider}

/**
 * @author: 李煌民
 * @date: 2021-12-06 18:27
 *        ${description}
 **/
class NullInstrumentationFactory extends InstrumentationFactory{
  override def create(): InstrumentationProvider = {
    new NullInstrumentation
  }
}
