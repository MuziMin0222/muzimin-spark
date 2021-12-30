package com.muzimin.configuration.job.output

/**
 * @author: 李煌民
 * @date: 2021-11-17 23:25
 *        ${description}
 **/
case class Output(
                   //Apache Cassandra是一个高度可扩展的高性能分布式数据库，用于处理大量商用服务器上的大量数据，提供高可用性，无单点故障。这是一种NoSQL类型的数据库
                   //cassandra: Option[Cassandra] = None,
                   //Amazon Redshift 是一种可轻松扩展的完全托管型 PB 级数据仓库服务，可与您现有的商业智能工具协作。它通过使用列存储技术和并行化多个节点的查询来提供快速的查询性能
                   //redshift: Option[Redshift] = None,
                   //redis是一个key-value存储系统
                   redis: Option[Redis] = None,
                   //数据文件空间
                   //segment: Option[Segment] = None,
                   //jdbc: Option[JDBC] = None,
                   ////jdbcquery: Option[JDBC] = None,
                   //file: Option[File] = None,
                   //kafka: Option[Kafka] = None,
                   //elasticsearch: Option[Elasticsearch] = None,
                   //hudi: Option[Hudi] = None,
                   hive: Option[Hive] = None
                 )
