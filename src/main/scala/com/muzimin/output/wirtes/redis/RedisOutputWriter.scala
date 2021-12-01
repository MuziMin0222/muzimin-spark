package com.muzimin.output.wirtes.redis

import com.muzimin.configuration.job.output.Redis
import com.muzimin.output.{Writer, WriterSessionRegistration}
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import com.redislabs.provider.redis._

import scala.util.parsing.json.JSONObject

/**
 * @author: 李煌民
 * @date: 2021-11-18 00:03
 *        ${description}
 **/
object RedisOutputWriter extends WriterSessionRegistration {
  def addConfToSparkSession(sparkBuilder: SparkSession.Builder, redisConf: Redis): Unit = {
    //配置redis的host
    sparkBuilder.config("redis.host", redisConf.host)

    //配置redis的端口号
    redisConf
      .port
      .foreach(
        _port => {
          sparkBuilder.config(s"redis.port", _port)
        }
      )

    //配置redis password/ACL权限问题
    redisConf
      .auth
      .foreach(
        _auth => {
          sparkBuilder.config("redis.auth", _auth)
        }
      )

    //配置redis的数据库序号
    redisConf
      .db
      .foreach(
        _db => {
          sparkBuilder.config("redis.db", _db)
        }
      )
  }
}

class RedisOutputWriter(props: Map[String, String], spark: SparkSession) extends Writer {
  private val log = LogManager.getLogger(this.getClass)

  case class RedisOutputProperties(keyColumn: String)

  //获取配置中的keyColumn参数的值赋值给样例类
  val redisOutputOptions = RedisOutputProperties(props("keyColumn"))

  //校验redis
  private def isRedisConfExist: Boolean = {
    spark
      .conf
      .getOption("redis.host")
      .isDefined
  }

  /**
   * 用户自定义将DataFrame写出到其他地方
   *
   * @param dataFrame DataFrame结果集
   */
  override def write(dataFrame: DataFrame): Unit = {
    if (isRedisConfExist) {
      val columns = dataFrame.columns.filter(_ != redisOutputOptions.keyColumn)

      import dataFrame.sparkSession.implicits._

      //将dataFrame中的数据转为key - value格式
      val redisDF: Dataset[(String, String)] = dataFrame
        .na.fill(0) //先将null值转为0
        .na.fill("") //再将null值转为空串
        .map(
          row => {
            row.getAs[Any](redisOutputOptions.keyColumn).toString -> JSONObject(row.getValuesMap(columns)).toString()
          }
        )

      log.info(s"Writting Dataframe into redis with key ${redisOutputOptions.keyColumn}")
      redisDF.sparkSession.sparkContext.toRedisKV(redisDF.toJavaRDD)

    } else {
      log.error("Redis配置不存在，请重新校验配置参数...")
    }
  }
}
