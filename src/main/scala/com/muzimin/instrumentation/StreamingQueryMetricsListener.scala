package com.muzimin.instrumentation

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQueryListener

/**
 * @author: 李煌民
 * @date: 2021-12-08 14:01
 *        ${description}
 **/
object StreamingQueryMetricsListener {
  val log: Logger = LogManager.getLogger(this.getClass)

  def init(spark: SparkSession, instrumentationProvider: InstrumentationProvider): Unit = {
    val listener = new StreamingQueryMetricsListener(instrumentationProvider)
    spark.streams.addListener(listener)
    log.info("初始化spark流式监听器...")
  }
}

class StreamingQueryMetricsListener(instrumentationProvider: InstrumentationProvider) extends StreamingQueryListener {
  //transient 告知 JPA 的提供者不要持久化任何属性
  @transient lazy val log = LogManager.getLogger(this.getClass)

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
    log.info("开始处理队列中的内容....")
  }

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    val numInputRows = event.progress.numInputRows
    instrumentationProvider.gauge(name = "InputEventsCount", value = numInputRows)

    val processedRowsPerSecond = event.progress.processedRowsPerSecond
    instrumentationProvider.gauge(name = "ProcessedEventsPerSecond", value = processedRowsPerSecond.toLong)
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
    event.exception match {
      case Some(e) =>
        instrumentationProvider.count(name = "QueryExceptionCounter", value = 1)
        log.error("Query failed with exception: " + e)
      case None =>
        instrumentationProvider.count(name = "QueryStopCounter", value = 1)
    }
  }
}
