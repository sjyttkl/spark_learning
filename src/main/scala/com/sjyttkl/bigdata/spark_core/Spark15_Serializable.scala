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
object Spark15_Serializable {
  def main(args: Array[String]): Unit = {

     val config :SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(config)

    val rdd :RDD[String] = sc.parallelize(Array("hadoop","spark","hive","songdongodng"))

    val search = new Search("h")

//    val match1:RDD[String] = search.getMatch1(rdd)
    val match1:RDD[String] = search.getMatch2(rdd)
    match1.collect().foreach(println)

    //释放资源
    sc.stop()
  }

}
//query

class Search(query: String) extends  java.io.Serializable {
  //过滤出包含字符串的数据
  def isMatch(s: String): Boolean = {
    s.contains(query)
  }

  //过滤出包含字符串的RDD
  def getMatch1(rdd: RDD[String]): RDD[String] = {
    rdd.filter(isMatch)//在这个方法中所调用的方法isMatch()是定义在Search这个类中的，实际上调用的是this. isMatch()，this表示Search这个类的对象，程序在运行过程中需要将Search对象序列化以后传递到Executor端
  }

  //过滤出包含字符串的RDD
  def getMatch2(rdd: RDD[String]): RDD[String] = {
    val q = query //成员属性，字符串本身就会序列化，将类变量赋值给局部变量
    rdd.filter(x => x.contains(query))
  }

}