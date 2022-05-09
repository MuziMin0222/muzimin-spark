package com.muzimin.step

import com.muzimin.configuration.step.Step
import com.muzimin.step.step_actions.{Code, Sql}
import com.muzimin.utils.FileUtils

/**
 * @author: 李煌民
 * @date: 2022-01-11 15:43
 *        ${description}
 **/
object StepActionFactory {
  def getStepAction(
                     configuration: Step,
                     showPreviewLine: Int,
                     cacheOnPreview: Option[Boolean],
                     showQuery: Option[Boolean]
                   ): StepAction[_] = {
    configuration.sql match {
      case Some(sqlQuery) => {
        Sql(sqlQuery, configuration.dataFrameName, showPreviewLine, cacheOnPreview, showQuery)
      }
      case _ => {
        //执行sql文件
        configuration.file match {
          case Some(filePath) => {
            Sql(FileUtils.readConfigurationFile(filePath), configuration.dataFrameName, showPreviewLine, cacheOnPreview, showQuery)
          }
          case None => {
            configuration.classpath match {
              case Some(classPath) => {
                Code(classPath, configuration.dataFrameName, configuration.params, showPreviewLine, cacheOnPreview)
              }
              case None => {
                throw new Exception("每一个执行步骤都需要指定sql文件/sql语句，或者实现RichProcessJob接口的代码")
              }
            }
          }
        }
      }
    }
  }
}
