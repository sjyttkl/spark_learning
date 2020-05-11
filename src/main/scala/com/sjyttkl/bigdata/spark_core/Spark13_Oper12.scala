package com.sjyttkl.bigdata.spark_core

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, Partitioner, SparkConf, SparkContext}

/**
  * Create with: com.sjyttkl.bigdata.spark 
  * author: sjyttkl
  * E-mail: 695492835@qq.com
  * date: 2019/8/25 0:59 
  * version: 1.0
  * description:  KEY-VALUE类型
  */
//自定义分区器
object Spark13_Oper12 {
  def main(args: Array[String]): Unit = {
    var config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建Spark上下文对象

    var sc: SparkContext = new SparkContext(config)
    //map算子,后面2 是两个分区，一定有两个，最后一个分区会把剩下的数据存完。2）和文件分区不一样，文件分区最少会有两个。

    //从指定
    //    var listRDD: RDD[Int] = sc.makeRDD( 1 to 16) //这里的to 是包含  10的， unto 是不包含10 的, 后面的2 是确定分区数
    var listRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3), ("c", 2), ("d", 4)))
    var listRDD2: RDD[(String, Int)] = sc.makeRDD(List(("a", 4), ("b", 6), ("c", 8), ("c", 0), ("d", 22)))
//    var partRDD: RDD[(String, Int)] = listRDD.partitionBy(new MyPartitioner(3))//自定义分区
    //一般是为了传入一个分区器 new org.apache.spark.HashParititon(2)
    var partRDD: RDD[(String, Int)] = listRDD.partitionBy(new org.apache.spark.HashPartitioner(2))

    //    listRDD.groupByKey()  //中间存在shuffle
    //    listRDD.reduceByKey() //中间存在shuffle，但是在之前已经存在一个简单的预聚合操作，导致效率高，性能会得到提高
    //aggregateByKey 主要是为了求 每个分区的最大值，再对每个分区间进行相加
//    var aggrRDD: RDD[(String, Int)] = listRDD.aggregateByKey(0)(math.max(_, _), _ + _) //第一个是初始值（看需求，第二个参数，表示两两求最大值。
    //https://blog.csdn.net/qaz_125/article/details/83140649 参考
    var aggrRDD: RDD[(String, Int)] =listRDD.aggregateByKey(0)(_+_,_+_)//这里其实就是分区内相加，分区间也相加，就是求wordCount

    var foldRDD: RDD[(String, Int)]  = listRDD.foldByKey(0)(_+_)//这里比aggregateByKey 少了一个参数，表示分区内核分区间的算法可以使用一样的,s是对上面的简化

    //使用combineByKeyWithClassTag 计算平均值，没有初始值，但是要变成 （key,value）_1，再次有分区内和分区间
    var combine: RDD[(String, (Int, Int))] = listRDD.combineByKeyWithClassTag((_, 1), (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1), (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
    val result = combine.map{case(key,value)=>(key,value._1/value._2.toDouble)}
    var combineRDD: RDD[(String, Int)] = listRDD.combineByKeyWithClassTag(x => x, (x: Int, y) => x + y, (x: Int, y: Int) => x + y)//统计wordCount
    //    listRDD.combineByKeyWithClassTag(x=>x ,_+_,_+_)这个会报错，因为它推断不出来类型

    var sort: RDD[(String, Int)] = listRDD.sortByKey(true) //升序排序,false降序排序

    //mapValue 只对 value操作，不看key,一般用在分组之后，求和，最大值等？
    var mapValueRDD: RDD[(String, String)] = listRDD.mapValues(_ + "||")
    //join ,两个数据集进行join,如果不存在则不出现,性能比较低
    var joinRDD: RDD[(String, (Int, Int))] = listRDD.join(listRDD2)

    //cogroup，如果不存在，还是会出现的，为空,，还需要注意顺序
    var cogroupRDD: RDD[(String, (Iterable[Int], Iterable[Int]))] = listRDD.cogroup(listRDD2)

    //注意区别
    combineRDD.foreach(println)
//    aggrRDD.saveAsTextFile("output")

    println(listRDD.countByKey())//

  }
}
//声明分区器
  class MyPartitioner(partitions:Int) extends Partitioner{
  override  def numPartitions: Int = {
  partitions
  }
  override def getPartition(key: Any): Int = {
    1
  }
  }

