package com.sjyttkl.bigdata.spark_sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * Create with: com.sjyttkl.bigdata.spark_sql
  * author: sjyttkl
  * E-mail: 695492835@qq.com
  * date: 2019/9/22 11:34
  * version: 1.0
  * description: spark_sql
  */
object SparkSQL03_transform1 {
  def main(args: Array[String]): Unit = {

    //SparkSQL
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")

    //创建spark上下文对象
    //SparkSession
    // var session: SparkSession = new SparkSession(config)
    val spark_session: SparkSession = SparkSession.builder().config(config).getOrCreate()

    //创建RDD
    var rdd: RDD[(Int, String)] = spark_session.sparkContext.makeRDD(List((1, "zhagnshan"), (2, "宋冬冬")))
    //进行转换前，需要引入隐式转换规则。
    //这里spark_session不是包的名字，是SparkSession的对象
    import spark_session.implicits._   //无论自己是否需要隐式转换，最好还是加上


    //RDD --直接转-  DataSet，要求RDD，同时要有结构，同时要有类型
    var userRDD: RDD[User2] = rdd.map {
      case (id, name) => {
        User2(id, name)
      }

    }
    var userDS: Dataset[User2] = userRDD.toDS()
    val rdd1:RDD[User2] = userDS.rdd

    rdd1.foreach(println)

    spark_session.stop()


  }
}

//创建样例类，DataSet需要类型
case class User2(id:Int,name:String);
