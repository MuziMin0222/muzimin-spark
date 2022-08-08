package com.muzimin.configuration.job

import com.muzimin.configuration.job.catalog.Catalog
import com.muzimin.configuration.job.input.Input
import com.muzimin.configuration.job.output.Output
import com.muzimin.configuration.job.output_conf.OutputConf
import com.muzimin.configuration.job.step.Step
import com.muzimin.input.Reader

/**
 * @author : 李煌民
 * @date : 2021-09-14 11:31
 *       ${description}
 **/
case class Configuration(
                          steps: Option[List[Step]],
                          inputs: Option[Map[String, Input]],
                          variables: Option[Map[String, String]], //sql中要配置的变量
                          outputConf: Option[OutputConf],
                          outputConfs: Option[Map[String, OutputConf]],
                          output: Option[List[Output]],
                          catalog: Option[Catalog], //Spark Catalog 配置
                          cacheOnPreview: Option[Boolean],
                          showQuery: Option[Boolean],
                          var logLevel: Option[String], //日志级别配置
                          var appName: Option[String], //任务的appName配置
                          var showPreviewLines: Option[Int] //DataFrame show的行数
                        ) {

  //require 表示step文件必须要有，否则程序直接退出
  require(steps.isDefined)
  //如果日志级别没有传入，默认是INFO
  logLevel = Option(logLevel.getOrElse("INFO"))
  //设置默认的AppName
  appName = Option(appName.getOrElse("MuziMinSpark"))
  showPreviewLines = Option(showPreviewLines.getOrElse(0))

  //将配置文件中配置的input，转为临时表，临时表的表名就是map的key
  def getReaders: Seq[Reader] = {
    inputs.getOrElse(Map())
      .map {
        case (name, input) => {
          input.getReader(name)
        }
      }.toSeq
  }

  override def toString: String = {
    s"""
       |steps -> $steps
       |inputs -> $inputs
       |variables -> $variables
       |outputConf -> $outputConf
       |outputConfs -> $outputConfs
       |outPut -> $output
       |catalog -> $catalog
       |cacheOnPreview -> $cacheOnPreview
       |showQuery -> $showQuery
       |logLevel -> $logLevel
       |appName -> $appName
       |showPreviewLines -> $showPreviewLines
       |""".stripMargin
  }
}
