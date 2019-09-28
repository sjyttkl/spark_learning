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
object Spark09_Oper8 {
  def main(args: Array[String]): Unit = {
    var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建Spark上下文对象

    var sc: SparkContext = new SparkContext(config)
    //map算子,后面2 是两个分区，一定有两个，最后一个分区会把剩下的数据存完。2）和文件分区不一样，文件分区最少会有两个。

    //从指定数据集合中进行抽样处理。
    var listRDD: RDD[Int] = sc.makeRDD( 1 to 16) //这里的to 是包含  10的， unto 是不包含10 的, 后面的2 是确定分区数

//    var SampleRDD: RDD[Int] = listRDD.sample(false, 0.4, 1)//不放回
    var SampleRDD: RDD[Int] = listRDD.sample(true, 4, 1)//放回抽样，可重复

    SampleRDD.collect().foreach(println)



  }
}
