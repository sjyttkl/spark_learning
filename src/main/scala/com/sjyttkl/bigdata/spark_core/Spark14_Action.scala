package com.sjyttkl.bigdata.spark_core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * Create with: com.sjyttkl.bigdata.spark
  * author: sjyttkl
  * E-mail: 695492835@qq.com
  * date: 2019/8/26 11:34
  * version: 1.0
  * description: Action算子 ，会马上计算，不会延迟就算。注意和转换算子进行区分。
  */
object Spark14_Action {
  def main(args: Array[String]): Unit = {
    var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建Spark上下文对象

    var sc: SparkContext = new SparkContext(config)
    //map算子,后面2 是两个分区，一定有两个，最后一个分区会把剩下的数据存完。2）和文件分区不一样，文件分区最少会有两个。

    //从指定
    //    var listRDD: RDD[Int] = sc.makeRDD( 1 to 16) //这里的to 是包含  10的， unto 是不包含10 的, 后面的2 是确定分区数
    var rdd1: RDD[Int] = sc.makeRDD(1 to 10, 2)
    var rdd2: RDD[(String, Int)] = sc.makeRDD(Array(("a", 1), ("b", 3), ("c", 2)))
    var rdd3: RDD[Int] = sc.parallelize(1 to 10)
    var rdd4: RDD[Int] = sc.parallelize(Array(1, 4, 3, 2, 4, 5))

    var reduceRdd1: Int = rdd1.reduce(_ + _)
    val reduceRdd2 = rdd2.reduce((x, y) => (x._1 + y._1, x._2 + y._2))
    println(reduceRdd1) //聚合操作 ,会现在 分区内聚合，再在分间进行聚合
    println(reduceRdd2) //聚合操作
    println(rdd3.collect()) //在驱动程序中，以数组的形式返回数据集中的所有元素。将RDD的内容收到Driver端进行打印
    println(rdd3.count()) //返回元素的个数
    println(rdd3.first()) //取第一个元素
    rdd3.takeOrdered(3).foreach(println) //取第三个元素，不排序
    rdd4.takeOrdered(3).foreach(println) //取排序后 前三个元素

    println(rdd1.aggregate(0)(_+_,_+_)) //将该RDD所有元素相加得到结果 55,把，先分区内相加，在分区间相加
    println(rdd1.aggregate(10)(_+_,_+_)) //将该RDD所有元素相加得到结果85 ，但是初始值会同时作用在分区内核分区间，所以会多10 出来 和 aggregateByKey 不一样的。
    println(rdd1.fold(10)(_+_)) //将所有元素相加得到结果

  }
}
