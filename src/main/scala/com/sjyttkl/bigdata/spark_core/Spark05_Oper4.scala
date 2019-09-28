package com.sjyttkl.bigdata.spark_core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create with: com.sjyttkl.bigdata.spark 
  * author: sjyttkl
  * E-mail: 695492835@qq.com
  * date: 2019/8/25 0:59 
  * version: 1.0
  * description:  
  */
object Spark05_Oper4 {
  def main(args: Array[String]): Unit = {
    var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建Spark上下文对象

    var sc: SparkContext = new SparkContext(config)
    //map算子
    var listRDD: RDD[List[Int]] = sc.makeRDD(Array(List(1,2),List(3,4))) //这里的to 是包含  10的， unto 是不包含10 的, 后面的2 是确定分区数

    //flatMap
    //1,2,3,4
    var flatMapRDD: RDD[Int] = listRDD.flatMap(datas => datas)

    flatMapRDD.collect().foreach(println)



  }
}
