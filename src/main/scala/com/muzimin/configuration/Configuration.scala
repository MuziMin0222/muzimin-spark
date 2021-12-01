package com.muzimin.configuration

import com.muzimin.configuration.job.Output

/**
 * @author : 李煌民
 * @date : 2021-09-14 11:31
 *       ${description}
 **/
case class Configuration(
                          metrics: Option[Seq[String]],
                          //inputs: Option[Map[String, Input]],
                          variables: Option[Map[String, String]],
                          //instrumentation: Option[Instrumentation],
                          output: Option[Output],
                          outputs: Option[Map[String, Output]],
                          //catalog: Option[Catalog],
                          cacheOnPreview: Option[Boolean],
                          showQuery: Option[Boolean],
                          //streaming: Option[Streaming],
                          //periodic: Option[Periodic],
                          var logLevel: Option[String],
                          var showPreviewLines: Option[Int],
                          var explain: Option[Boolean],
                          var appName: Option[String],
                          var continueOnFailedStep: Option[Boolean],
                          var cacheCountOnOutput: Option[Boolean],
                          var ignoreDeequValidations: Option[Boolean],
                          var failedDFLocationPrefix: Option[String]
                        ){
  override def toString: String = {
    s"""
       |metrics: $metrics,
       |variables: $variables,
       |output: $output,
       |outputs: $outputs,
       |cacheOnPreview: $cacheOnPreview,
       |showQuery: $showQuery,
       |logLevel: $logLevel,
       |showPreviewLines: $showPreviewLines,
       |explain: $explain,
       |appName: $appName,
       |continueOnFailedStep: $continueOnFailedStep,
       |ignoreDeequValidations: $ignoreDeequValidations,
       |failedDFLocationPrefix: $failedDFLocationPrefix
       |""".stripMargin
  }
}
