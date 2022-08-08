package com.muzimin.output.wirtes.file

import com.muzimin.configuration.job.output_conf.File
import com.muzimin.output.Writer
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row}

/**
 * @author: 李煌民
 * @date: 2022-01-12 15:13
 *        ${description}
 **/
class FileOutputWriter(props: Map[String, Any], outputFile: Option[File]) extends Writer {
  /**
   * 用户自定义将DataFrame写出到其他地方
   *
   * @param dataFrame DataFrame结果集
   */
  override def write(dataFrame: DataFrame): Unit = {
    val writer: DataFrameWriter[Row] = dataFrame.write

    //如果没有指定输出文件的格式，默认为csv格式
    props.get("format").asInstanceOf[Option[String]] match {
      case Some(format) => {
        writer.format(format)
      }
      case None => {
        writer.format("csv")
      }
    }

    //写出时用什么分区，默认不分区
    props.get("partitionBy").asInstanceOf[Option[Seq[String]]] match {
      case Some(partitionBy) => {
        writer.partitionBy(partitionBy: _*)
      }
      case None =>
    }

    //写出时指定什么方式写出，overwrite，append，ignore，error，errorifexists，default
    props.get("saveMode").asInstanceOf[Option[String]] match {
      case Some(saveMode) => {
        writer.mode(saveMode)
      }
      case None =>
    }

    //文件输出的额外配置
    props.get("extraOptions").asInstanceOf[Option[Map[String, String]]] match {
      case Some(options) => {
        writer.options(options)
      }
      case None =>
    }

    //如果在step.yaml中给了path值，那么就认为是写到一个文件中，比如xml，excel
    val path = props.get("path").asInstanceOf[Option[String]] match {
      case Some(path) => {
        outputFile.get.dir + java.io.File.separator + path
      }
      case None => outputFile.get.dir
    }

    writer.save(path)
  }
}
