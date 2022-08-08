package com.muzimin.output.wirtes.hive

import com.muzimin.configuration.job.output.Output
import com.muzimin.configuration.job.output_conf.Hive
import com.muzimin.output.{Writer, WriterSessionRegistration}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

/**
 * @author: 李煌民
 * @date: 2021-11-18 22:21
 *        ${description}
 **/
object HiveOutputWriter extends WriterSessionRegistration {
  def addConfToSparkSession(sparkSessionBuilder: SparkSession.Builder, hiveConf: Hive): Unit = {
    sparkSessionBuilder.enableHiveSupport()
  }
}

class HiveOutputWriter(outputConfig: Output, outputHive: Option[Hive], spark: SparkSession) extends Writer {
  /**
   * 用户自定义将DataFrame写出到其他地方
   *
   * @param dataFrame DataFrame结果集
   */
  override def write(dataFrame: DataFrame): Unit = {
    val hiveOutputConf: Hive = outputHive.get
    val props: Map[String, Any] = outputConfig.outputOptions
    val sourceTb: String = outputConfig.dataFrameName

    val db_name: String = hiveOutputConf.dbName match {
      case Some(db_name) => {
        db_name
      }
      case _ => {
        "default"
      }
    }
    val tb_name: String = hiveOutputConf.tbName
    log.info(s"DBName：${db_name} ---> TBName：${tb_name}")

    val columns: Option[String] = props.get("columns").asInstanceOf[Option[String]]
    val partitions: Option[String] = props.get("partitions").asInstanceOf[Option[String]]
    log.info(s"columns: ${columns} ---> partitions: ${partitions}")

    val columnOrder: String = getOrder(columns)
    val concatColumnOrder: String = getOrderType(columns)
    log.info(s"columnOrder ${columnOrder} ---> concatColumnOrder ${concatColumnOrder}")

    val partitionOrder: String = getOrder(partitions)
    val concatPartitionOrder: String = getOrderType(partitions)
    log.info(s"partitionOrder: ${partitionOrder} ---> concatPartitionOrder: ${concatPartitionOrder}")

    val rowFormatSerde: String = props.get("rowFormatSerde").asInstanceOf[Option[String]] match {
      case Some(serde) => {
        serde
      }
      case _ => {
        "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
      }
    }
    log.info(s"rowFormatSerde: ${rowFormatSerde}")

    val collectionDelim: String = props.get("collectionDelim").asInstanceOf[Option[String]] match {
      case Some(delim) => {
        delim
      }
      case _ => {
        "\u0002"
      }
    }
    val fieldDelim: String = props.get("fieldDelim").asInstanceOf[Option[String]] match {
      case Some(delim) => {
        delim
      }
      case _ => {
        "\u0001"
      }
    }
    val mapkeyDelim: String = props.get("mapkeyDelim").asInstanceOf[Option[String]] match {
      case Some(delim) => {
        delim
      }
      case _ => {
        "\u0003"
      }
    }
    log.info(s"collectionDelim: ${collectionDelim} ---> fieldDelim: ${fieldDelim} ---> mapkeyDelim: ${mapkeyDelim}")

    val inputFormat: String = props.get("inputFormat").asInstanceOf[Option[String]] match {
      case Some(format) => {
        format
      }
      case _ => {
        "org.apache.hadoop.mapred.TextInputFormat"
      }
    }
    val outputFormat: String = props.get("outputFormat").asInstanceOf[Option[String]] match {
      case Some(format) => {
        format
      }
      case _ => {
        "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
      }
    }
    log.info(s"inputFormat: ${inputFormat} ---> outputFormat: ${outputFormat}")

    val location: String = props.get("location").asInstanceOf[Option[String]] match {
      case Some(location) => {
        location
      }
      case _ => {
        throw new Exception("需要指定一个表的location位置")
      }
    }
    log.info(s"location: ${location}")

    val partitionValue: Map[String, Any] = props.get("partitionValue").asInstanceOf[Option[Map[String, Any]]] match {
      case Some(partitionValue) => {
        partitionValue
      }
      case _ => {
        Map()
      }
    }
    log.info(s"partitionValue: ${partitionValue}")
    val concatPartitionValue: String = getStaticPartition(partitionValue)
    log.info(s"concatPartitionValue: ${concatPartitionValue}")

    val tbType = hiveOutputConf.tbType
    log.info(s"数据插入hive表的类型==》 ${tbType}")
    tbType match {
      case "full" => {
        log.info(s"开始创建外部表: $tb_name")
        val createSql =
          s"""
             |CREATE EXTERNAL TABLE IF NOT EXISTS `$db_name`.`$tb_name`
             |  $concatColumnOrder
             |ROW FORMAT SERDE
             |  '$rowFormatSerde'
             |WITH SERDEPROPERTIES (
             |  'colelction.delim'='$collectionDelim',
             |  'field.delim'='$fieldDelim',
             |  'mapkey.delim'='$mapkeyDelim',
             |  'serialization.format'='$fieldDelim')
             |STORED AS INPUTFORMAT
             |  '$inputFormat'
             |OUTPUTFORMAT
             |  '$outputFormat'
             |LOCATION
             |  '$location'
             |""".stripMargin

        log.info(s"创建表语句如下：$createSql")
        spark.sql(createSql)

        log.info(s"数据插入全量表--> ${outputHive.get.tbName}")
        val insertSql =
          s"""
             |INSERT OVERWRITE TABLE ${db_name}.${tb_name}
             |SELECT
             |  ${columnOrder}
             |FROM ${sourceTb}
             |""".stripMargin

        log.info(s"开始执行sql:$insertSql")
        spark.sql(insertSql)
      }
      case "static" => {
        log.info(s"开始创建外部表 ${tb_name}")
        val createSql =
          s"""
             |CREATE EXTERNAL TABLE IF NOT EXISTS `$db_name`.`$tb_name`
             |  $concatColumnOrder
             |PARTITIONED BY
             |  $concatPartitionOrder
             |ROW FORMAT SERDE
             |  '$rowFormatSerde'
             |WITH SERDEPROPERTIES (
             |  'colelction.delim'='$collectionDelim',
             |  'field.delim'='$fieldDelim',
             |  'mapkey.delim'='$mapkeyDelim',
             |  'serialization.format'='$fieldDelim')
             |STORED AS INPUTFORMAT
             |  '$inputFormat'
             |OUTPUTFORMAT
             |  '$outputFormat'
             |LOCATION
             |  '$location'
             |""".stripMargin

        log.info(s"创建表语句如下： $createSql")
        spark.sql(createSql)

        log.info(s"静态分区插入表： ${outputHive.get.tbName}")
        val insertSql =
          s"""
             |INSERT OVERWRITE TABLE ${db_name}.$tb_name
             |PARTITION$concatPartitionValue
             |SELECT
             |  $columnOrder
             |FROM $sourceTb
             |""".stripMargin
        log.info(s"开始执行sql：$insertSql")
        spark.sql(insertSql)
      }
      case "dynamic" => {
        log.info(s"开始创建外部表 ${tb_name}")
        val createSql =
          s"""
             |CREATE EXTERNAL TABLE IF NOT EXISTS `$db_name`.`$tb_name`
             |  $concatColumnOrder
             |PARTITIONED BY
             |  $concatPartitionOrder
             |ROW FORMAT SERDE
             |  '$rowFormatSerde'
             |WITH SERDEPROPERTIES (
             |  'colelction.delim'='$collectionDelim',
             |  'field.delim'='$fieldDelim',
             |  'mapkey.delim'='$mapkeyDelim',
             |  'serialization.format'='$fieldDelim')
             |STORED AS INPUTFORMAT
             |  '$inputFormat'
             |OUTPUTFORMAT
             |  '$outputFormat'
             |LOCATION
             |  '$location'
             |""".stripMargin

        log.info(s"创建表语句如下： $createSql")
        spark.sql(createSql)

        log.info(s"动态分区插入表：${outputHive.get.tbName}")
        val insertSql =
          s"""
             |INSERT OVERWRITE TABLE $db_name.$tb_name
             |PARTITION($partitionOrder)
             |SELECT
             |  $columnOrder,$partitionOrder
             |FROM $sourceTb
             |""".stripMargin

        log.info(s"开始执行sql：$insertSql")
        spark.sql("set hive.exec.dynamic.partition=true")
        spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
        spark.sql(insertSql)
      }
      case _ => {
        throw new Exception("表的插入方式不对，请重新校验。（full，static，dynamic的三种之一）")
      }
    }
  }

  def getOrder(orderType: Option[String]): String = {
    val list = new ListBuffer[String]

    if (orderType.isDefined) {
      orderType match {
        case Some(columns) => {
          columns.split("\\|", -1).foreach(
            fieldType => {
              val filed = fieldType.split(" ")(0)
              list.append(filed)
            }
          )
        }
        case _ => {
          throw new Exception("列类型不正确，请按特定格式在step文件中进行配置")
        }
      }
      list.mkString(",")
    } else {
      ""
    }
  }

  def getOrderType(orderType: Option[String]): String = {
    var result: String = ""

    if (orderType.isDefined) {
      orderType match {
        case Some(columns) => {
          columns.split("\\|", -1).foreach(
            fieldType => {
              val arr = fieldType.split(" ")
              val columnName = arr(0)
              val columnType = arr(1)
              result += s"`$columnName` $columnType,"
            }
          )

          if (result.contains(",")) {
            result = result.reverse.replaceFirst(",", "").reverse
          }
          log.info("muzimin logger | keywords handled: ".concat(result))
          result = "(".concat(result).concat(")")
        }
        case _ => {
          throw new Exception("列类型不正确，请按特定格式在step文件中进行配置")
        }
      }

      result
    } else {
      ""
    }
  }

  def getStaticPartition(partitionValue: Map[String, Any]): String = {
    val list = new ListBuffer[String]()

    partitionValue.foreach{
      case (k,v) => {
        list.append(s"$k='$v'")
      }
      case _ =>
    }

    "(".concat(list.mkString(",")).concat(")")
  }
}
