package com.muzimin.input.obs

import cn.hutool.crypto.SecureUtil
import cn.hutool.json.JSONUtil
import com.muzimin.input.Reader
import com.obs.services.ObsClient
import com.obs.services.model.{ListObjectsRequest, ObjectListing}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, get_json_object}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
 * @author: 李煌民
 * @date: 2022-08-18 12:03
 *        ${description}
 **/
trait ObsDataInputBase {
  def getObsClient(ak: String, sk: String, endPoint: String): ObsClient = {
    new ObsClient(ak, sk, endPoint)
  }

  def setRequest(bucketName: String, prefix: Option[String]): ListObjectsRequest = {
    val request = new ListObjectsRequest(bucketName)
    request.setMaxKeys(100000)

    prefix match {
      case Some(v) => {
        request.setPrefix(v)
      }
      case None =>
    }

    request
  }

  def getJsonObject(schemaColumnOrder: Option[String]): ListBuffer[Column] = {
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
