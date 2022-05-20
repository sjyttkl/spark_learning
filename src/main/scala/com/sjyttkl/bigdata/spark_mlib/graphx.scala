package com.sjyttkl.bigdata.spark_mlib

import org.apache.spark.{SparkConf, SparkContext,HashPartitioner}

/**
 * Create with: com.sjyttkl.bigdata.spark_mlib
 * author: songdongdong
 * E-mail: songdongdong@weidian.com
 * date: 2021/3/29 18:01
 * version: 1.0
 * description: 
 */
object graphx {
  def main(args: Array[String]): Unit = {

    val config:SparkConf = new SparkConf().setMaster("local[*]").setAppName("graphx") //"local[*]"
    //1,创建上下文对象
    val sc = new SparkContext(config)
    val links = sc.parallelize(List(("A",List("B","C")),("B",List("A","C")),("C",List("A","B","D")),("D",List("C")))).persist() //.partitionBy(new HashPartitioner(100)).

    var ranks=links.mapValues(v=>1.0)

    for (i <- 0 until 10) {
      val contributions=links.join(ranks).flatMap {
        case (pageId,(links,rank)) => links.map(dest=>(dest,rank/links.size))
      }
      ranks=contributions.reduceByKey((x,y)=>x+y).mapValues(v=>0.15+0.85*v)
    }

    ranks.sortByKey().collect().foreach(println)
    var seq = Seq("1","2","3","4","5","6","7")
    println(seq.take(20))


  }
}
