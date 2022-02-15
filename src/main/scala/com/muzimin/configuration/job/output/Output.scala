package com.muzimin.configuration.job.output

/**
 * @author: 李煌民
 * @date: 2021-11-17 23:25
 *        ${description}
 **/
case class Output(
                   redis: Option[Redis] = None,
                   hive: Option[Hive] = None,
                   file: Option[File] = None,
                   jdbc: Option[JDBC] = None
                 )
