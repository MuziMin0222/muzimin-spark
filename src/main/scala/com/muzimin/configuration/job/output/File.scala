package com.muzimin.configuration.job.output

/**
 * @author: 李煌民
 * @date: 2022-01-12 15:15
 *        ${description}
 **/
case class File(dir: String) {
  require(Option(dir).isDefined, "文件输出目录，是必须强制要求存在的")
}
