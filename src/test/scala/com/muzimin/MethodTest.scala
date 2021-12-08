package com.muzimin

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.muzimin.configuration.Configuration
import org.junit.Test

import scala.collection.mutable

/**
 * @author: 李煌民
 * @date: 2021-11-17 22:57
 *        ${description}
 **/
class MethodTest {
  @Test
  def testReadValue(): Unit = {
    val mapper = new ObjectMapper()
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

    mapper.registerModule(DefaultScalaModule)
  }
}

case class demo(aaa:String)
