package com.sjyttkl.bigdata.spark_sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Create with: com.sjyttkl.bigdata.spark_sql
  * author: sjyttkl
  * E-mail: 695492835@qq.com
  * date: 2019/9/22 11:34
  * version: 1.0
  * description: spark_sql
  */
object SparkSQL02_SQL {
  def main(args: Array[String]): Unit = {

    //SparkSQL
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")

    //创建spark上下文对象
    //SparkSession
    // var session: SparkSession = new SparkSession(config)
    val session: SparkSession = SparkSession.builder().config(config).getOrCreate()

    //读取数据，构建DataFrame
    var frame: DataFrame = session.read.json("in/user.json")

    //将DataFrame转成一张表
    frame.createOrReplaceTempView("user")
    session.sql("select * from user").show

    //展示数据
    frame.show()

    session.stop()


  }
}
