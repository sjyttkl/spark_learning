package com.sjyttkl.bigdata.spark_mlib

import org.apache.spark.mllib.linalg.{Matrix, Matrices}

/**
 * Create with: com.sjyttkl.bigdata.spark_mlib
 * author: songdongdong
 * E-mail: songdongdong@weidian.com
 * date: 2020/8/11 14:42
 * version: 1.0
 * description: 局部矩阵 https://www.cnblogs.com/swordfall/p/9456222.html#auto_id_1
 */
object local_matrix {
  def main(args: Array[String]): Unit = {

    //创建密矩阵（（1.0，2.0），（3.0, 4.0），（5.0, 6.0））
    val dm: Matrix = Matrices.dense(3, 2, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0))

    println(dm)
  }
}
