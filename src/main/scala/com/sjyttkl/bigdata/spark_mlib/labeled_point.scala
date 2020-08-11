package com.sjyttkl.bigdata.spark_mlib

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD

/**
 * Create with: com.sjyttkl.bigdata.spark_mlib
 * author: songdongdong
 * E-mail: songdongdong@weidian.com
 * date: 2020/8/11 14:38
 * version: 1.0
 * description:  标记点是将密集向量或者稀疏向量与应答标签相关联。
 * 在MLlib中，标记点用于监督学习算法。MLlib使用double类型存储标签，所以我们能在回归和分类中使用标记点。
 * 如果只有两种分类，可以使用二分法，一个标签要么是1.0，要么是0.0。如果有很多分类，标签应该从零开始：0、1、2....
 */
object labeled_point {
  def main(args: Array[String]): Unit = {

    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName) //"local[*]"
    //1,创建上下文对象
    val sc = new SparkContext(config)

    //使用标签1.0和一个密集向量创建一个标记点
    val pos = LabeledPoint(1.0, Vectors.dense(1.0, 0.0, 3.0))
    //使用标签0.0和一个疏向量创建一个标记点
    val neg = LabeledPoint(0.0, Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0)))

    println(pos)
    println(neg)

    //用稀疏的训练数据做练习是很常见的，好在MLlib支持读取存储在LIBSVM格式中的训练例子。LIBSVM格式是一种每一行表示一个标签稀疏特征向量的文本格式，其格式如下： label index1:value1 index2:value2 ...
    //LIBSVM是林智仁教授等开发设计的一个简单、易用和快速有效的SVM模式识别与回归的软件包。MLlib已经提供了MLUtils.loadLibSVMFile方法读取存储在LIBSVM格式文本文件中的训练数据，见如下代码：


    val examples: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc, "in/sample_libsvm_data.txt")
    examples.collect().foreach(println)

  }
}
