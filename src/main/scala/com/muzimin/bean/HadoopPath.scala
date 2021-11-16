package com.muzimin.bean

import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}

/**
 * @author : 李煌民
 * @date : 2021-09-14 12:19
 *       ${description}
 **/
case class HadoopPath(path: Path, fs: FileSystem) {
  def open: FSDataInputStream = {
    fs.open(path)
  }

  def getName: String = {
    path.getName
  }
}
