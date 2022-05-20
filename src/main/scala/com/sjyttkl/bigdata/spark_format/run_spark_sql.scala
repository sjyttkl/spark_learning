package com.sjyttkl.bigdata.spark_format

/**
 * Create with: run_spark_sql
 * author: songdongdong
 * E-mail: songdongdong@weidian.com
 * date: 2020/4/27 20:24
 * version: 1.0
 * description: spark_sql  代码
 */


import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{udf, col}
import org.apache.spark.{SparkConf, SparkContext}


object run_spark_sql {

  //店铺基本信息，标题分词
  val QUERY_SHOPNAME_SPLIT_WORD =
    """select
      |      seller_id,
      |      shop_name,
      |      split_char,
      |      split_word,
      |      pt
      |    from
      |      hzsearch.r_dd_ls_seller_info_feature_split
      |    ---where
      |    ---  pt = date_sub(current_date, 1)
      |    where pt= '%1$s'
      |       """.stripMargin



  def main(args: Array[String]): Unit = {

    val sparkOperation = SparkOperation(this.getClass.getName)
    val spark = sparkOperation.spark

  }
}


class SparkOperation(appName: String) {

  val spark = SparkSession
    .builder()
    .appName(appName)
    .enableHiveSupport() //.master("local[*]")
    .getOrCreate()

  def insert_to_table_dt(table_name: String, dt: String, result: DataFrame): Unit = {
    result.createOrReplaceTempView("tmp_result")
    var sql = s"insert overwrite table $table_name partition(pt='$dt') select * from tmp_result "
    spark.sql(sql).show(20)
  }

  def insert_overwrite_table(table_name: String, result: DataFrame): Unit = {
    println("准备插入数据了。。。。。")
    result.createOrReplaceTempView("tmp_result")
    var pt = getYersterday()
    var sql = s"insert overwrite table $table_name partition(pt='${pt}' ) select * from tmp_result"
    spark.sql(sql) //.show(20)
    println("数据导入完毕......")
  }

  //可以对分区进行合并，可以合并成一个。参考https://blog.csdn.net/weixin_37944880/article/details/86694829 ,  https://blog.csdn.net/qq_39160721/article/details/82387328
  def insert_overwrite_table_repartitions(table_name: String, result: DataFrame, partitions: Int, pt: String): Unit = {
    println("准备插入数据了。。。。。")
    //var pt = getYersterday()
    //var select_val = spark.sql("select * from tmp_result")
    val result_temp = result.repartition(partitions).persist() //分区保留100个
    result_temp.createOrReplaceTempView("tmp_result")
    var sql = s"insert overwrite table $table_name partition(pt='${pt}' ) select * from tmp_result "
    spark.sql(sql) //.show(20)
    println("数据导入完毕......")
  }

  def insert_overwrite_table_version(table_name: String, result: DataFrame, version: String): Unit = {
    println("准备插入数据了。。。。。")
    result.createOrReplaceTempView("tmp_result")
    var pt = getYersterday()
    var sql = s"insert overwrite table $table_name partition(pt='${pt}',version='${version}' ) select * from tmp_result"
    spark.sql(sql) //.show(20)
    println("数据导入完毕......")
  }

  def insert_to_into_table(table_name: String, result: DataFrame): Unit = {
    println("准备插入数据了。。。。。")
    result.createOrReplaceTempView("tmp_result")
    var pt = getYersterday()
    var sql = s"insert into table $table_name partition(pt='${pt}' ) select * from tmp_result "
    spark.sql(sql) //.show(20)
    println("数据导入完毕......")
  }

  def query_from_table(table_name: String): DataFrame = {
    var sql = s" select * from  $table_name where pt = date_sub(current_date, 1) "
    spark.sql(sql)
  }

  def query_from_table_version(table_name: String, version: String): DataFrame = {
    var sql = s" select * from  $table_name  where pt = date_sub(current_date, 1) and version=$version "
    spark.sql(sql)
  }

  def query_from_table_pt(table_name: String, pt: String): DataFrame = {
    var sql = s" select *  from $table_name  where pt = $pt "
    println(sql)
    spark.sql(sql)
  }

  def query_from_table_pt_version(table_name: String, pt: String, version: String): DataFrame = {
    var sql = s" select * from  $table_name  where pt = $pt and version= $version "
    spark.sql(sql)
  }

  //获得表 最近一天的时间分区
  def get_pt_lastPartitions(tableName: String): String = {
    var frame: DataFrame = spark.sql("show partitions " + tableName)
    frame = frame.withColumn("pt", pt(col("partition")))
    frame = frame.sort(col("pt").desc).drop("partition")
    "'" + frame.take(1)(0).mkString("") + "'"
  }

  val pt = udf((pt: String) => {
    pt.split("=")(1)
  })

  def getYersterday(): String = {
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DATE, -1)
    val time: Date = calendar.getTime //获取时间
    val newtime: String = new SimpleDateFormat("yyyy-MM-dd").format(time) //设置格式并且对时间格式化
    newtime
  }

  def getNowMonthStart(): String = {

    var cal: Calendar = Calendar.getInstance();
    var df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    cal.set(Calendar.DATE, 1)
    var period = df.format(cal.getTime) //本月第一天
    period
  }

}

object SparkOperation {

  def apply(appName: String) = new SparkOperation(appName)
}
