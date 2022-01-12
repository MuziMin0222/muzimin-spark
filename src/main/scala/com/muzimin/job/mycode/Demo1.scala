package com.muzimin.job.mycode

import com.muzimin.job.RichProcessJob
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * @author: 李煌民
 * @date: 2022-01-11 17:26
 *        ${description}
 **/
class Demo1 extends RichProcessJob {
  override def run(sparkSession: SparkSession, dataFrameName: String, params: Option[Map[String, String]]): Unit = {

    val myudf = udf(
      (column:String) => {
        column + "这是UDF产生的列"
      }
    )

    println("dataFrameName ====> " + dataFrameName)

    println(params)

    val df = sparkSession.table("ratings")
        .withColumn("userId_new",myudf(col("userId")))

    df.show(false)

    df.createOrReplaceTempView(dataFrameName)
  }
}
