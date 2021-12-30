package com.muzimin

import java.io.File

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.muzimin.configuration.job.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.DataStreamReader
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

  @Test
  def testIsDedind(): Unit = {
    val file = Option.apply(new File(""))

    println(Seq(file).find(
      x => {
        x.isDefined
      }
    ).get)
  }

  @Test
  def testSparkReadStream(): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("demo")
      .getOrCreate()

    val streamReader = spark.readStream.format("csv").load("/Users/muzimin/study/test_files/a.csv")

    streamReader.show()

    spark.close()
  }

  @Test
  def testRequire(): Unit = {

    require(1 < 2,"这是必须是True，程序才能往下执行")

    println("这是执行的程序")
  }

  @Test
  def testSystemSeq():Unit = {
    val value = "/user/muzimin/a.txt"
    println(value.substring(value.lastIndexOf(File.separator) + 1))
  }
}

case class demo(aaa: String)
