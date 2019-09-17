package com.sjyttkl.bigdata.spark

import java.util

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Create with: com.sjyttkl.bigdata.spark
  * author: sjyttkl
  * E-mail: 695492835@qq.com
  * date: 2019/9/17 11:34
  * version: 1.0
  * description: 自定义广播器
  */
object Spark22_Var {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(config)

    var rdd1: RDD[(Int, String)] = sc.makeRDD(List((1, "a"), (2, "b"), (3, "c")))

//    var rdd2: RDD[(Int, Int)] = sc.makeRDD(List((1, 1), (2, 2), (3, 3)))
    ////    (1,(a,1))
    ////    (3,(a,3))
    ////    (2,(b,2))
    //    val joinRDD :RDD[(Int,(String ,Int))] = rdd1.join(rdd2) //效率很差，设计到shuffle的过程，笛卡尔乘积，效率太慢
    //    joinRDD.foreach(println)







    val list = List((1,1),(2,2),(3,3))
    //可以使用广播变量减少数据的传输
    //构建广播变量
    var broadcast: Broadcast[List[(Int, Int)]] = sc.broadcast(list) //共享的只读变量，减少数据传输的总量
    val resultRDD:RDD[(Int,(String,Any))] = rdd1.map{ //map没有shuffle
      case(key,value) =>{
        var v2:Any = null
        //2，使用广播变量
        for (t<-broadcast.value){
          if(key == t._1){
            v2 = t._2
          }
        }
        (key,(value,v2))
      }
    }
    resultRDD.foreach(println)
    //释放资源
    sc.stop()
  }


}