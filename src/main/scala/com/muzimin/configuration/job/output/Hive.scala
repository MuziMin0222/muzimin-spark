package com.muzimin.configuration.job.output

/**
 * @author: 李煌民
 * @date: 2021-11-18 22:16
 *        ${description}
 **/
case class Hive(
                 dbName: Option[String],
                 tbName: String,
                 tbType: String
               )
