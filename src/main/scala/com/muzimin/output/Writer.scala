package com.muzimin.output

import org.slf4j.LoggerFactory
import org.apache.spark.sql.DataFrame

/**
 * @author: 李煌民
 * @date: 2021-11-18 00:02
 *        ${description}
 **/
trait Writer extends Serializable {
  /**
   * 用户自定义将DataFrame写出到其他地方
   *
   * @param dataFrame DataFrame结果集
   */
  def write(dataFrame: DataFrame): Unit

  val log = LoggerFactory.getLogger(this.getClass)
}
