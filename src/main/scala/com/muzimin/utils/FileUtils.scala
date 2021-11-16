package com.muzimin.utils

import java.io.{BufferedReader, File, FileNotFoundException, InputStreamReader}
import java.util.stream.Collectors

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.muzimin.bean.HadoopPath
import com.muzimin.input.file.FileType
import org.apache.commons.io.FilenameUtils
import org.apache.commons.text.StringSubstitutor
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

/**
 * @author : 李煌民
 * @date : 2021-09-14 12:03
 *       ${description}
 **/
object FileUtils {
  def main(args: Array[String]): Unit = {
    println(StringSubstitutor.replace("fileContents,,,", getEnvProperties().asJava))
  }

  /**
   * 将本地文件夹/文件中的所有文件放入List中
   *
   * @param dir 文件地址
   * @return
   */
  def getListOfLocalFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else if (d.isFile) {
      List(d)
    } else {
      throw new FileNotFoundException(s"No Files to Run ${dir}")
    }
  }

  /**
   * 通过传入的文件类型进行对象转换
   *
   * @param extension
   * @return
   */
  def getObjectMapperByExtension(extension: String): Option[ObjectMapper] = {
    extension match {
      case "json" => Option(new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false))
      case "yaml" | "yml" | _ => Option(new ObjectMapper(new YAMLFactory()).configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false))
    }
  }

  /**
   * 传入文件名，获取后缀，进行对象转换
   *
   * @param fileName
   * @return
   */
  def getObjectMapperByFileName(fileName: String): Option[ObjectMapper] = {
    val extension = FilenameUtils.getExtension(fileName)
    getObjectMapperByExtension(extension)
  }

  /**
   * 获取运行所在机器的环境信息
   *
   * @return
   */
  def getEnvProperties(): Map[String, String] = {
    val envAndSystemProperties = System.getProperties().asScala ++= System.getenv().asScala
    envAndSystemProperties.toMap
  }

  /**
   * 获取前缀信息
   *
   * @param envProperties
   * @return
   */
  def getFilesPathPrefix(envProperties: Option[Map[String, String]]): Option[String] = {
    envProperties.getOrElse(getEnvProperties()).get("CONFIG_FILES_PATH_PREFIX")
  }

  /**
   * 将HDFS路径封装成 HadoopPath对象
   *
   * @param path
   * @return
   */
  def getHadoopPath(path: String): HadoopPath = {
    val hadoopConf = SparkSession.builder().getOrCreate().sessionState.newHadoopConf()

    val file = new Path(path)

    val fs = file.getFileSystem(hadoopConf)
    HadoopPath(file, fs)
  }

  /**
   * 将HDFS中的文件进行读取封装成字符串
   *
   * @param path
   * @return
   */
  def readFileWithHadoop(path: String): String = {
    val hadoopPath = getHadoopPath(path)

    val fsFile = hadoopPath.open

    val reader = new BufferedReader(new InputStreamReader(fsFile))
    reader.lines.collect(Collectors.joining("\n"))
  }

  def readConfigurationFile(path: String): String = {
    val envAndSystemProperties = getEnvProperties()
    val prefix = getFilesPathPrefix(Option.apply(envAndSystemProperties)).getOrElse("")

    val fileContents = readFileWithHadoop(prefix + path)
    StringSubstitutor.replace(fileContents, envAndSystemProperties.asJava)
  }

  def isLocalDirectory(path: String): Boolean = {
    new File(path).isDirectory
  }

  def isLocalFile(path: String): Boolean = {
    new File(path).isFile
  }

  def getFileFormat(path: String): String = {
    FileType.getFileType(path) match {
      case FileType.json | FileType.jsonl => "json"
      case FileType.csv => "csv"
      case _ => "parquet"
    }
  }
}
