package com.muzimin.output.wirtes.hive

import com.muzimin.configuration.job.output.Hive
import com.muzimin.output.WriterSessionRegistration
import org.apache.spark.sql.SparkSession

/**
 * @author: 李煌民
 * @date: 2021-11-18 22:21
 *        ${description}
 **/
object HiveOutputWriter extends WriterSessionRegistration{
  def addConfToSparkSession(sparkSessionBuilder: SparkSession.Builder, hiveConf: Hive): Unit = {
    sparkSessionBuilder.enableHiveSupport()
  }
}
