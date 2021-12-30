package com.muzimin.input.file

import org.apache.commons.io.FilenameUtils

/**
 * @author : 李煌民
 * @date : 2021-09-15 20:08
 *
 **/
object FileType extends Enumeration {
  type TableType = Value
  val parquet, json, jsonl, csv, excel = Value

  def isValidFileType(s: String): Boolean = values.exists(_.toString == s)

  /**
   * 不属于这些枚举文件默认csv文本文件
   *
   * @param path
   * @return
   */
  def getFileType(path: String): TableType = {
    val extension = FilenameUtils.getExtension(path)
    if (isValidFileType(extension)) FileType.withName(extension) else csv
  }
}
