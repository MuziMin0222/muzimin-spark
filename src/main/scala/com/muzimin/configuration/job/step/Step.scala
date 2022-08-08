package com.muzimin.configuration.job.step

/**
 * @author: 李煌民
 * @date: 2021-12-30 18:21
 *        ${description}
 **/
case class Step(
                 dataFrameName: String, //执行后的DataFrame名称
                 sql: Option[String], //sql 语句
                 file: Option[String], //sql文件
                 classpath: Option[String], //自己定义的数据处理逻辑的类全名
                 params: Option[Map[String, String]],
                 var ignoreOnFailures: Option[Boolean]
               ) {

  //如果没有传入ignoreOnFailures  默认是False
  ignoreOnFailures = Option(ignoreOnFailures.getOrElse(false))
}
