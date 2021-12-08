package com.muzimin.instrumentation

/**
 * @author: 李煌民
 * @date: 2021-12-06 17:35
 *        ${description}
 **/
trait InstrumentationFactory extends Serializable {
  def create(): InstrumentationProvider
}
