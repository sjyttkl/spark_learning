package com.sjyttkl.bigdata.Spark_streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Create with: com.sjyttkl.bigdata.Spark_streaming 
  * author: sjyttkl
  * E-mail:  695492835@qq.com
  * date: 2019/9/29 12:53
  * version: 1.0
  * description:  transform
  */
object SparkStreaming07_transform {
  def main(args: Array[String]): Unit = {

    //SparkSQL
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming01_WordCount")

    var streamingContext: StreamingContext = new StreamingContext(config, Seconds(3)) //3 秒钟，伴生对象，不需要new

    //保存数据的状态，设置检查点路径
    streamingContext.sparkContext.setCheckpointDir("cp")

    var socketLineStreaming :ReceiverInputDStream[String] = streamingContext.socketTextStream("linux1", 9999) //一行一行的接受

    //转换

    //TODO 代码（Driver)
    socketLineStreaming.map{
      case x=>{
        //TODO 代码（Executer)
        x
      }
    }

//    //TODO 代码（Driver)
//    socketLineStreaming.transform{
//      case rdd=>{
//        //TODO 代码（Driver)(m  运行采集周期 次)
//        rdd.map{
//          case x=>{
//            //TODO 代码 （Executer)
//            x
//          }
//        }
//      }
//    }

    socketLineStreaming.foreachRDD(rdd=>{
      rdd.foreach(println)
    })

    //打印结果
//    stateDStream.print()

    //streamingContext.stop()  //不能停止我们的采集功能

    //启动采集器
    streamingContext.start()

    //Drvier等待采集器停止，
    streamingContext.awaitTermination()

    //nc -lc 99999   linux 下往9999端口发数据。

  }
}

