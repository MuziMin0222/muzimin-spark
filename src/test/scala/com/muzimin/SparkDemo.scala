package com.muzimin

import cn.hutool.crypto.SecureUtil
import cn.hutool.json.JSONUtil
import com.obs.services.ObsClient
import com.obs.services.model.{ListObjectsRequest, ObjectListing}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.jdbc2.JDBCSaveMode

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.io.Source

/**
 * @author: 李煌民
 * @date: 2022-03-16 16:03
 *        ${description}
 **/
case class a(id: String, name: String, pv: Int, uv: Int)

object SparkDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()

    demo01(spark)

    spark.close()
  }

  def demo01(spark: SparkSession): Unit = {
    val obsClient = new ObsClient("YPIYKCXYV3DGM2HAHN6W", "RXW1F8UHNbE6lb38CeiLelVdNqq1jci9FZa4xpzR", "obs.ap-southeast-3.myhuaweicloud.com")

    val request = new ListObjectsRequest("hw-poc-etl")
    request.setMaxKeys(100000)

    Option("pre-ods/issmart/obs-issmart-action/2022-05-07/") match {
      case Some(v) => {
        request.setPrefix(v)
      }
      case None =>
    }

    val map = new mutable.HashMap[String, String]()
    Source.fromInputStream(obsClient.getObject("hw-poc-etl", "pre-ods/issmart/obs-issmart-action/2022-05-07/_success").getObjectContent)
      .getLines()
      .foreach(
        line => {
          val arr = line.split("\t", -1)
          map.put(arr(0), arr(1))
        }
      )

    val list = new ListBuffer[String]

    val sb = new mutable.HashMap[String, String]

    var result: ObjectListing = null
    do {
      result = obsClient.listObjects(request)

      for (obsObject <- result.getObjects) {
        val objectKey = obsObject.getObjectKey

        if (objectKey.contains("txt")) {
          val jsonContent = Source.fromInputStream(obsClient.getObject("hw-poc-etl", objectKey).getObjectContent).mkString

          val key = objectKey.substring(objectKey.lastIndexOf("/") + 1)
          val md5 = SecureUtil.md5(jsonContent)

          sb.put(key, md5)

          for (jsonObject <- JSONUtil.parseArray(jsonContent)) {
            list.append(jsonObject.toString)
          }
        }
      }
      request.setMarker(result.getNextMarker)

    } while (result.isTruncated)

    println(map.equals(sb))

    import spark.implicits._
    spark.sparkContext.makeRDD(list)
      .toDF("temp")
      .show(false)
  }

  def demo(spark: SparkSession): Unit = {
    import spark.implicits._
    spark.sparkContext.makeRDD(Seq(a("1", "lhm", 12, 43))).toDF
      .write
      .format("org.apache.spark.sql.execution.datasources.jdbc2")
      .options(
        Map(
          "savemode" -> JDBCSaveMode.Update.toString,
          "driver" -> "com.mysql.jdbc.Driver",
          "url" -> "jdbc:mysql://localhost:3306/test",
          "user" -> "root",
          "password" -> "Lhm18779700731!",
          "dbtable" -> "tb_b",
          "useSSL" -> "false",
          "duplicateIncs" -> "pv,uv",
          "showSql" -> "true"
        )
      ).save()
  }
}
