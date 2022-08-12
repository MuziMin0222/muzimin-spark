package com.muzimin.input.obs

import cn.hutool.json.{JSONArray, JSONUtil}
import com.muzimin.input.Reader
import com.obs.services.ObsClient
import com.obs.services.model.{ListObjectsRequest, ObjectListing}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.io.Source
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.functions._

/**
 * @author: 李煌民
 * @date: 2022-08-12 14:20
 *        ${description}
 **/
case class ObsJsonInput(
                         name: String,
                         ak: String,
                         sk: String,
                         endPoint: String,
                         bucketName: String,
                         prefix: Option[String],
                         schemaColumnOrder: Option[String]
                       ) extends Reader {

  override def read(spark: SparkSession): DataFrame = {
    val obsClient = new ObsClient(ak, sk, endPoint)

    val request = new ListObjectsRequest(bucketName)
    request.setMaxKeys(100000)

    prefix match {
      case Some(v) => {
        request.setPrefix(v)
      }
      case None =>
    }

    val list = new ListBuffer[String]

    var result: ObjectListing = null
    do {
      result = obsClient.listObjects(request)

      for (obsObject <- result.getObjects) {
        val objectKey = obsObject.getObjectKey

        if (objectKey.contains("json")) {
          val jsonContent = Source.fromInputStream(obsClient.getObject("hw-sinobase", objectKey).getObjectContent).mkString

          for (jsonObject <- JSONUtil.parseArray(jsonContent)) {
            list.append(jsonObject.toString)
          }
        }
      }
      request.setMarker(result.getNextMarker)

    } while (result.isTruncated)

    import spark.implicits._
    spark
      .sparkContext
      .makeRDD(list)
      .toDF("temp")
      .select(getJsonObject: _*)
  }

  def getJsonObject: ListBuffer[Column] = {
    val columnList = new ListBuffer[Column]

    schemaColumnOrder match {
      case Some(order) => {
        order.split(",", -1)
          .foreach(
            column => {
              columnList.append(get_json_object(col("temp"), "$." + column).as(column))
            }
          )
      }
      case None => {
        columnList.append(col("temp"))
      }
    }

    columnList
  }
}
