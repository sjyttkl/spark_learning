package com.sjyttkl.bigdata.spark_ml


import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.sql.Row
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.feature.IDF
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
/**
 * Create with: com.sjyttkl.bigdata.spark_mlib
 * author: songdongdong
 * E-mail: songdongdong@weidian.com
 * date: 2020/8/11 14:13
 * version: 1.0
 * description:
 */
object CreatModel {

  case class RawDataRecord(category: String, text: String)

  def main(args: Array[String]): Unit = {
//    def main(args: Array[String]): Unit = {
//      val spark = SparkSession
//        .builder()
//        .appName(this.getClass.getSimpleName)
//        .enableHiveSupport().master("local[*]")
//        .getOrCreate()
//
//    }

    val config = new SparkConf().setAppName("createModel").setMaster("local[4]");
    val sc = new SparkContext(config);
    val spark = SparkSession.builder().config(config).config("spark.sql.warehouse.dir", "warehouse/dir").getOrCreate();
    import spark.implicits._
    //分数据
    val Array(srcDF, testDF) = sc.textFile("D:\\decstop\\testFiles\\sougou").map {
      x =>
        val data = x.split(",")
        RawDataRecord(data(0), data(1))
    }.toDF().randomSplit(Array(0.7, 0.3))

    //分词
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    val wordsData = tokenizer.transform(srcDF)
    wordsData.show(false)
    val testtokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    val testwordsData = testtokenizer.transform(testDF)

    //文档词频
    val hashingTF =
      new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(100)
    val featurizedData = hashingTF.transform(wordsData)

    val testhashingTF =
      new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(100)
    val testfeaturizedData = testhashingTF.transform(testwordsData)

    //逆文档词频
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData)

    val testidf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val testidfModel = testidf.fit(testfeaturizedData)
    val testrescaledData = testidfModel.transform(testfeaturizedData)
    rescaledData.show(false)
    //转换成贝叶斯的输入格式
    val trainDataRdd = rescaledData.select($"category", $"features").map {
      case Row(label: String, features: Vector) =>
        LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
    }

    val testtrainDataRdd = testrescaledData.select($"category", $"features").map {
      case Row(label: String, features: Vector) =>
        LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
    }

    val model = new NaiveBayes().fit(trainDataRdd)

    val predictions = model.transform(testtrainDataRdd)
    println("predictln out:");
    predictions.show();
    model.write.overwrite().save("resoult")

    //模型评估
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println("accuracy out :")
    println("Accuracy:" + accuracy)

  }}
