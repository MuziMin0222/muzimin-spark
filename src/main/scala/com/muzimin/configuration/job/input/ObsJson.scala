package com.muzimin.configuration.job.input

import com.muzimin.input.Reader
import com.muzimin.input.obs.ObsJsonInput

/**
 * @author: 李煌民
 * @date: 2022-08-12 14:15
 *        ${description}
 **/
case class ObsJson(
                    ak: String,
                    sk: String,
                    endPoint: String,
                    bucketName: String,
                    prefix: Option[String],
                    schemaColumnOrder: Option[String] //逗号分割的JSON列名
                  ) extends InputConfig {
  override def getReader(name: String): Reader = {
    ObsJsonInput(name, ak, sk, endPoint, bucketName, prefix,schemaColumnOrder)
  }
}
