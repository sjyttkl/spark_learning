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
object Spark12_Oper11 {
  def main(args: Array[String]): Unit = {
    var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建Spark上下文对象

    var sc: SparkContext = new SparkContext(config)
    //map算子,后面2 是两个分区，一定有两个，最后一个分区会把剩下的数据存完。2）和文件分区不一样，文件分区最少会有两个。

    //从指定
    var listRDD: RDD[Int] = sc.makeRDD( 1 to 16) //这里的to 是包含  10的， unto 是不包含10 的, 后面的2 是确定分区数


    // coalesce 和 repartition 的区别
//     listRDD.coalesce() //如果需要shuffle,需要传入true,默认是不shuffle
//    listRDD.repartition() // 底层也是调用coalesce() ，但是默认是shuffle
//    listRDD.sortBy() //可以传入  降序或者是升序
//    listRDD.union(listRDD)//求并集
//    listRDD.subtract(listRDD)//求差集
//    listRDD.intersection(listRDD)//求交集
//    listRDD.cartesian(listRDD)//笛卡尔积
//    listRDD.zip(listRDD)//拉链（1,2），这里的拉链，必须保持每个分区数据一样多。和scala里的不一样的。同时分区数也要一样才行。
//    coalesceRDD.saveAsTextFile("output")

     listRDD.saveAsTextFile("output1")
     //listRDD.saveAsSequenceFile("output2")#将数据集中的元素以Hadoop sequencefile的格式保存到指定的目录下，可以使HDFS或者其他Hadoop支持的文件系统。
     listRDD.saveAsObjectFile("output3") //byteWrite


  }
}
