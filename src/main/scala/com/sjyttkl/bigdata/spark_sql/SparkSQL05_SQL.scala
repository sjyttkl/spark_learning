package com.sjyttkl.bigdata.spark_sql

import java.io.File

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Create with: com.sjyttkl.bigdata.spark_sql
  * author: sjyttkl
  * E-mail: 695492835@qq.com
  * date: 2019/9/25 15:21
  * version: 1.0
  * description:
  */
object SparkSQL05_SQL {
  def main(args: Array[String]): Unit = {
    //使用Hive的操作
    //val warehouseLocation: String = new File("spark-warehouse").getAbsolutePath //如果是内置的、需指定hive仓库地址，若使用的是外部Hive，则需要将hive-site.xml添加到ClassPath下。
    val sparkSession = SparkSession.builder()
      .appName("Spark Hive Example")
      .master("local[*]")
     // .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()
     val rdd = sparkSession.sparkContext.textFile("in/person.txt")
    //var frame: DataFrame = sparkSession.read.json("in/user.json")

    //整理数据，ROW类型
    val rowrdd = rdd.map(line=>{
      val fields = line.split(",")
      val id = fields(0).toLong
      val name = fields(1)
      val age = fields(2).toInt
      val facevalue = fields(3).toDouble
      Row(id,name,age,facevalue)
    })

    //scheme:定义DataFrame里面元素的数据类型，以及对每个元素的约束
    val structType = StructType(List(
      StructField("id",LongType,true),
      StructField("name",StringType,true),
      StructField("age",IntegerType,true),
      StructField("facevalue",DoubleType,true)
    ))
    //将rowrdd和structType关联,因为文本 类 没有结构，所以
    val df:DataFrame = sparkSession.createDataFrame(rowrdd,structType)
    //创建一个视图
    df.createOrReplaceTempView("Tperson")
    //基于注册的视图写SQL
    val res:DataFrame = sparkSession.sql("SELECT name,age FROM Tperson ORDER BY age asc")

    res.show()

    sparkSession.stop()

  }
}
