package com.muzimin

import java.io.File

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.muzimin.configuration.job.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.DataStreamReader
import org.junit.Test

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

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
  def testRequire(): Unit = {

    require(1 < 2, "这是必须是True，程序才能往下执行")

    println("这是执行的程序")
  }

  @Test
  def testSystemSeq(): Unit = {
    val value = "/user/muzimin/a.txt"
    println(value.substring(value.lastIndexOf(File.separator) + 1))

    println(System.nanoTime())
    println(System.currentTimeMillis())
  }

  @Test
  def testMap(): Unit = {
    val props: Map[String, Any] = Map(
      "columns" -> "a string|b string",
      "fieldDelim" -> 1234,
      "location" -> "hdfs://demo"
    )

    val map: Map[String, String] = Map()

    val columns: Option[String] = props.get("columns").asInstanceOf[Option[String]]
    println(columns.isDefined)

    val partitions: Option[String] = props.get("partitions").asInstanceOf[Option[String]]
    println(partitions.isDefined)
    println(getOrder(partitions))
    println(getOrderType(partitions))
    println(getOrderType(columns))

    println(getStaticPartition(map))

    val valueMap = Map(
      "year" -> "aaa","month" -> "12"
    )
    println(getStaticPartition(valueMap))
  }

  def getOrder(orderType: Option[String]): String = {
    val list = new ListBuffer[String]

    if (orderType.isDefined) {
      orderType match {
        case Some(columns) => {
          columns.split("\\|", -1).foreach(
            fieldType => {
              val filed = fieldType.split(" ")(0)
              list.append(filed)
            }
          )
        }
        case _ => {
          throw new Exception("列类型不正确，请按特定格式在step文件中进行配置")
        }
      }
      list.mkString(",")
    } else {
      ""
    }
  }

  def getOrderType(orderType: Option[String]): String = {
    var result: String = ""

    if (orderType.isDefined) {
      orderType match {
        case Some(columns) => {
          columns.split("\\|", -1).foreach(
            fieldType => {
              val arr = fieldType.split(" ")
              val columnName = arr(0)
              val columnType = arr(1)
              result += s"`$columnName` $columnType,"
            }
          )

          if (result.contains(",")) {
            result = result.reverse.replaceFirst(",", "").reverse
          }
          result = "(".concat(result).concat(")")
        }
        case _ => {
          throw new Exception("列类型不正确，请按特定格式在step文件中进行配置")
        }
      }

      result
    } else {
      ""
    }
  }

  def getStaticPartition(partitionValue: Map[String, String]): String = {
    val list = new ListBuffer[String]()
    try {
      partitionValue.foreach(
        map => {
          val key = map._1
          val value = map._2
          list.append(s"$key = '$value'")
        }
      )
    } catch {
      case e: Exception => {
        throw new Exception(s"getStaticPartition该方法在处理${partitionValue}出现了问题")
      }
    }

    "(".concat(list.mkString(",")).concat(")")
  }
}

case class demo(aaa: String)
