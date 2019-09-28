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
object Spark10_Oper9 {
  def main(args: Array[String]): Unit = {
    var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建Spark上下文对象

    var sc: SparkContext = new SparkContext(config)
    //map算子,后面2 是两个分区，一定有两个，最后一个分区会把剩下的数据存完。2）和文件分区不一样，文件分区最少会有两个。

    //从指定
    var listRDD: RDD[Int] = sc.makeRDD( List(1,2,2,23,3,3,4,4,4,4,6,7,7,0)) //这里的to 是包含  10的， unto 是不包含10 的, 后面的2 是确定分区数

    //distinct  对数据进行去重，但是因为它会导致数据去重后减少，所以可以改变默认的分区数量
    //一个分区，就是一个任务（task)。一个任务，会分配到一个execuator
    var distinctRDD: RDD[Int] = listRDD.distinct(2)
//    distinctRDD.collect().foreach(println//控制台打印，没有保存之前的顺序    401622337, 数据打乱重组，shuffle

    distinctRDD.saveAsTextFile("output")//数据 进行了shuffle 打乱重组，没有保存之前的顺序，每个分区存的数据和之前不一样了。
   //在spark中所有转换算子中，没有shuffle则速度快。


  }
}
