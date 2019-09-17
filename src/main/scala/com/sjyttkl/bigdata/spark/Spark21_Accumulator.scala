package com.sjyttkl.bigdata.spark

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Create with: com.sjyttkl.bigdata.spark
  * author: sjyttkl
  * E-mail: 695492835@qq.com
  * date: 2019/9/17 11:34
  * version: 1.0
  * description: 自定义累加器
  */
object Spark21_Accumulator {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(config)

    val dataRDD: RDD[String] = sc.makeRDD(List("Hadoop","hive","Habsde","scala","hello"),2)

    //    var sum :Int  = 0
    //    //使用累加器来共享变量，来累加数据
    //
    //    //创建累加器对象
    //    var accumulator: LongAccumulator = sc.longAccumulator

    // TODO 创建累加器
    val wordAccumulator =  new WordAccumulator
    // TODO 注册累加器
    sc.register(wordAccumulator)

    dataRDD.foreach {
      case word => {
        // TODO 执行累加器的累加功能
        wordAccumulator.add(word)
      }
    }
    // TODO 获取累加器的值
    print("sum =  " + wordAccumulator.value)
    //释放资源
    sc.stop()
  }
}

//声明累加器
//1. 继承一个累加器抽象类AccumulatorV2 ，
// 2. 实现抽象方法
//声明(创建）累加器
class WordAccumulator extends AccumulatorV2[String, util.ArrayList[String]] {

  val list = new util.ArrayList[String]()

  //当前累加器是否是初始化状态
  override def isZero: Boolean = {
    list.isEmpty
  }

  //复制累加器对象
  override def copy(): AccumulatorV2[String, util.ArrayList[String]] = {
    new WordAccumulator()
  }

  //重置累加器对象
  override def reset(): Unit = {
    list.clear()
  }

  //向累加器中增加数据
  override def add(v: String): Unit = {
    if (v.contains("h")) {
      list.add(v)
    }
  }

  //合并累加器
  override def merge(other: AccumulatorV2[String, util.ArrayList[String]]): Unit = {
    list.addAll(other.value)
  }

  //获取累加器的结果
  override def value: util.ArrayList[String] = {
    list
  }
}