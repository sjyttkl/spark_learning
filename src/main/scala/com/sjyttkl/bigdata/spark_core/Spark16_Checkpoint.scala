package com.sjyttkl.bigdata.spark_core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Create with: com.sjyttkl.bigdata.spark
  * author: sjyttkl
  * E-mail: 695492835@qq.com
  * date: 2019/9/8 11:34
  * version: 1.0
  * description: RDD中的函数传递
  */
object Spark16_Checkpoint {
  def main(args: Array[String]): Unit = {

     val config :SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(config)

    //设置jian检查点的保存目录
    sc.setCheckpointDir("cp")

     val rdd = sc.makeRDD(List(1,2,3,4,5))

    val mapRDD:RDD[(Int,Int)] = rdd.map((_,1))

    val reduceRDD = mapRDD.reduceByKey(_+_)
    mapRDD.checkpoint()

    reduceRDD.foreach(println)

    println(reduceRDD.toDebugString) //debug形式看血缘关系,如果从检查点抽取数据，将看不到血缘关系了。

    //释放资源
    sc.stop()
  }


}