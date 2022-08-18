package com.muzimin.input.obs

import cn.hutool.crypto.SecureUtil
import cn.hutool.json.JSONUtil
import com.muzimin.input.Reader
import com.obs.services.ObsClient
import com.obs.services.model.{ListObjectsRequest, ObjectListing}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
 * @author: 李煌民
 * @date: 2022-08-12 14:20
 *        ${description}
 **/
case class ObsTxtInput(
                        name: String,
                        ak: String,
                        sk: String,
                        endPoint: String,
                        bucketName: String,
                        prefix: Option[String],
                        schemaColumnOrder: Option[String]
                      ) extends Reader with ObsDataInputBase {

  override def read(spark: SparkSession): DataFrame = {
    val obsClient = getObsClient(ak, sk, endPoint)

    val request = setRequest(bucketName, prefix)

    val list = new ListBuffer[String]

    val dataMap = new mutable.HashMap[String, String]
    val successMap = new mutable.HashMap[String, String]()

    var result: ObjectListing = null
    do {
      result = obsClient.listObjects(request)

      for (obsObject <- result.getObjects) {
        val objectKey = obsObject.getObjectKey

        if (objectKey.contains("txt")) {
          val jsonContent = Source.fromInputStream(obsClient.getObject(bucketName, objectKey).getObjectContent).mkString

          val key = objectKey.substring(objectKey.lastIndexOf("/") + 1)
          val md5 = SecureUtil.md5(jsonContent)

          dataMap.put(key, md5)

          for (jsonObject <- JSONUtil.parseArray(jsonContent)) {
            list.append(jsonObject.toString)
          }
        }

        if (objectKey.contains("_success")) {
          Source.fromInputStream(obsClient.getObject(bucketName, objectKey).getObjectContent)
            .getLines()
            .foreach(
              line => {
                val arr = line.split("\t", -1)
                successMap.put(arr(0), arr(1))
              }
            )
        }
      }
      request.setMarker(result.getNextMarker)

    } while (result.isTruncated)

    import spark.implicits._
    spark
      .sparkContext
      .makeRDD(list)
      .toDF("temp")
      .select(getJsonObject(schemaColumnOrder): _*)
  }
}
