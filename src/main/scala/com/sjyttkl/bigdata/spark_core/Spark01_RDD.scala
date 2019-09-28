package com.sjyttkl.bigdata.spark_core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create with: com.sjyttkl.bigdata.spark 
  * author: sjyttkl
  * E-mail: 695492835@qq.com
  * date: 2019/8/24 23:56 
  * version: 1.0
  * description:  
  */
object Spark01_RDD {
  def main(args: Array[String]): Unit = {

    val config:SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount") //"local[*]"
    //1,创建上下文对象
    val sc = new SparkContext(config)
    //创建RDD
    //     1)从内存中长假RDD，底层就是实现parallelize
//    var listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5))

    //使用自定义分区
//    var listRDD_s: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5), 2)
    //2) 从内存中创建parallelize
//    var arrayRDD: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4))
    //3) 从外部存储中创建
    //默认情况下，可以读取项目路径，也可以读取hdfs
    //默认从文件读取的数据，都是字符类型
    //读取文件时，传递的参数为最小分区数，但是不一定是这个分区数，取决于hadoop读取文件的分片规则
    var fileRDD: RDD[String] = sc.textFile("in",3)
//    listRDD.collect().foreach(println)

    //将RDD的数据保存到文件中
    fileRDD.saveAsTextFile("output") //默认 电脑核数
  }
}
