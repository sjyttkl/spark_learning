package com.sjyttkl.bigdata.spark_sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf

/**
 * Create with: com.sjyttkl.bigdata.spark_sql
 * author: songdongdong
 * E-mail: songdongdong@weidian.com
 * date: 2020/8/13 19:08
 * version: 1.0
 * description: 
 */
object spark_define_sql {
  def main(args: Array[String]): Unit = {
    //SparkSQL
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)

    //创建spark上下文对象
    //SparkSession
    // var session: SparkSession = new SparkSession(config)
    val session: SparkSession = SparkSession.builder().config(config).getOrCreate()

    import session.implicits._
    var frame: DataFrame = session.read.json("in/user.json")

    session.udf.register("query_contains", query_contains(_: String, "我知道"))
    session.sql("select query_contains('我知道')").show()

    frame.show()
  }

  def query_contains(query: String, peoples: String) = {
    //println(peoples.mkString(","))
    var result = false
    if (query.trim.length == 0 || query.trim.equals(null) || query.trim == null) {
      result.toString
    }
    else {
//      var iktext: String = IkTokenizer.ikAnalyzer(query.trim, true)
      var iktext =""
      peoples.foreach(line => {
        //println(line)
        var line_2 = line.toString.trim
        //println(line_2)
        //var line_3 = line_2.substring(1, line_2.length - 1)
        //println(line_3)
        var bool: Boolean = iktext.contains(line_2)
        if (bool == true) {
          result = bool
        }

      })
      result.toString
    }
  }

}
