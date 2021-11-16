package com.muzimin

import com.muzimin.configuration.ConfigurationParser
import org.apache.log4j.LogManager

/**
 * @author : 李煌民
 * @date : 2021-08-18 11:25
 *       ${description}
 **/
object Application {
  val log = LogManager.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    log.info("start muzimin - parsing configuration")
    ConfigurationParser.parse(args)
  }
}
