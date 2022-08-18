package com.muzimin.configuration.job.input

import com.muzimin.input.Reader
import com.muzimin.input.obs.{ObsJsonInput, ObsTxtInput}

/**
 * @author: 李煌民
 * @date: 2022-08-12 14:15
 *        ${description}
 **/
case class ObsData(
                    ak: String,
                    sk: String,
                    endPoint: String,
                    bucketName: String,
                    fileFormat: String,
                    prefix: Option[String],
                    schemaColumnOrder: Option[String] //逗号分割的JSON列名
                  ) extends InputConfig {
  override def getReader(name: String): Reader = {
    fileFormat match {
      case "txt" => ObsTxtInput(name, ak, sk, endPoint, bucketName, prefix, schemaColumnOrder)
      case "json" => ObsJsonInput(name, ak, sk, endPoint, bucketName, prefix, schemaColumnOrder)
      case _ => throw new Exception("请选择正确的fileFormat，比如txt，json")
    }
  }
}
