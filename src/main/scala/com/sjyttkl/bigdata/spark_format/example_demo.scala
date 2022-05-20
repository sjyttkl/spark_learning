package com.sjyttkl.bigdata.spark_format

import java.util
import java.text.SimpleDateFormat
import java.time.LocalDate

import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions.{asc, col, collect_list, collect_set, concat_ws, countDistinct, desc, lit, row_number, split, trim, udf, when}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType}
import run_spark_sql._

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Random
/**
 * Create with: com.sjyttkl.bigdata.spark_format
 * author: songdongdong
 * E-mail: songdongdong@weidian.com
 * date: 2022/5/20 11:28
 * version: 1.0
 * description: 
 */
object example_demo {

  var stop_words: Array[String] = _
  var time = "test"

  def main(args: Array[String]): Unit = {

    val sparkOperation = SparkOperation(this.getClass.getName)
    val spark = sparkOperation.spark
    spark.conf.set("spark.sql.crossJoin.enabled", "true") //开启笛卡尔积
    spark.conf.set("spark.kryoserializer.buffer.max", "1g") //序列缓冲设置下
    //打印日志，检查是否成功设置
    println(spark.conf.get("spark.kryoserializer.buffer.max"))
    println(spark.conf.get("spark.sql.objectHashAggregate.sortBased.fallbackThreshold"))
    spark.conf.set("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", "15360")
    println(spark.conf.get("spark.sql.objectHashAggregate.sortBased.fallbackThreshold"))
    //spark.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "true") //true则会生成 success文件

    if (args.length != 0) {
      time = args(0)
      println(args(0))
    } else {
      println("重新跑吧")
    }

    //    // 按照seller_id进行分组，并且按照时间权重进行排序
    //    val w = Window.partitionBy("seller_id").orderBy($"time_decay_weight".desc) //$time_decay_weight  和 col("time_decay_weight") 是等价的
    //
    //    shop_info_all = shop_info_all.withColumn("time_decay_weight_desc", row_number.over(w)) //.drop($"time_decay_weight")


  }
}
