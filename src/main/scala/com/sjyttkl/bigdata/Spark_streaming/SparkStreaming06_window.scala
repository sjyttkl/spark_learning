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
  * description:  窗口操作
  */
object SparkStreaming06_window {
  def main(args: Array[String]): Unit = {

//    val ints = List(1,2,3,4,5,6)
//
//    //滑动,窗口为2
//    var intses: Iterator[List[Int]] = ints.sliding(2,1)
//
//    for(list<- intses)
//      {
//        println(list.mkString(","))
//      }

    //SparkSQL
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming01_WordCount")

    var streamingContext: StreamingContext = new StreamingContext(config, Seconds(3)) //3 秒钟，伴生对象，不需要new

    //保存数据的状态，设置检查点路径
    streamingContext.sparkContext.setCheckpointDir("cp")

    //从kafka中采集数据
    var kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(streamingContext, "linux1:2181", "xiaodong", Map("xiaodong" -> 3))


    //窗口大小应该为采集周期的整数倍，窗口滑动步长也应该是采集周期的整数倍
    var windowDStream: DStream[(String, String)] = kafkaDStream.window(Seconds(9), Seconds(3))

    //将采集的数据进行分解（偏平化）
    var WordDstream: DStream[String] = windowDStream.flatMap(t =>t._2.split(" ")) //偏平化后，按照空格分割

    //将我们的数据进行转换方便分析
    var mapDstream: DStream[(String, Int)] = WordDstream.map((_, 1))

    //将转换后的数据聚合在一起处理
    var stateDStream: DStream[(String, Int)] = mapDstream.updateStateByKey {
      case (seq, buffer) => {
        var sum = buffer.getOrElse(0) + seq.sum
        Option(sum)
      }
    }

    //打印结果
    stateDStream.print()

    //streamingContext.stop()  //不能停止我们的采集功能

    //启动采集器
    streamingContext.start()

    //Drvier等待采集器停止，
    streamingContext.awaitTermination()

    //nc -lc 99999   linux 下往9999端口发数据。

  }
}

