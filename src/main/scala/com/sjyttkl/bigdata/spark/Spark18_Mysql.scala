package com.sjyttkl.bigdata.spark

import java.sql.{DriverManager, PreparedStatement}

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Create with: com.sjyttkl.bigdata.spark
  * author: sjyttkl
  * E-mail: 695492835@qq.com
  * date: 2019/9/8 11:34
  * version: 1.0
  * description: RDD中的函数传递
  */
object Spark18_Mysql {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文对象
    val sc = new SparkContext(config)

    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/test"
    val userName = "root"
    val passMd = "root"

    //创建jdbcRDD，方法数据库,查询数据
    //    val sql = "select id ,id2 from dim  where id >1 and id <1000"
    //    var jdbcRDD = new JdbcRDD(
    //      sc,
    //      () => {
    //        //获取数据库连接对象
    //        Class.forName(driver)
    //        DriverManager.getConnection(url, userName, passMd)
    //      },
    //      sql,
    //      1,
    //      3,
    //      2,
    //      (rs) =>{
    //        println(rs.getInt(1)+"  ,  " + rs.getInt(2))
    //      }
    //    )
    //    jdbcRDD.collect()
    //  保存数据
    var dataRDD: RDD[(String, Int)] = sc.makeRDD(List(("zongzhong", 23), ("lishi", 23)))
    //    Class.forName(driver)
    //    val conection = java.sql.DriverManager.getConnection(url, userName, passMd)
    //    dataRDD.foreach {
    //      case (userName, age) => {
    //        Class.forName(driver)
    //        val conection = java.sql.DriverManager.getConnection(url, userName, passMd)
    //        val sql = "insert into  stu (name,age)values ?,?"
    //        var statement: PreparedStatement = conection.prepareStatement(sql)
    //        var usern: Unit = statement.setString(1, userName)
    //        val pasword = statement.setString(2, passMd)
    //        statement.executeUpdate()
    //        statement.close()
    //        conection.close()
    //
    //      }
    //    }
    dataRDD.foreachPartition(datas => {//以分区作为循环，发送到 excutor上，如果有两个分区，则直接发送到excuator上
      Class.forName(driver)
      val conection = java.sql.DriverManager.getConnection(url, userName, passMd)
      datas.foreach {
        case (userName, age) => {
          Class.forName(driver)
          val conection = java.sql.DriverManager.getConnection(url, userName, passMd)
          val sql = "insert into  stu (name,age)values ?,?"
          var statement: PreparedStatement = conection.prepareStatement(sql)
          var usern: Unit = statement.setString(1, userName)
          val pasword = statement.setString(2, passMd)
          statement.executeUpdate()
          statement.close()
          conection.close()

        }
      }
    })

    //释放资源
    sc.stop()
  }


}