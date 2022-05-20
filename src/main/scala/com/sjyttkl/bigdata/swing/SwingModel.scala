package com.vdian.pickover.swing

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{asc, broadcast, col, collect_list, collect_set, concat_ws, count, countDistinct, current_date, datediff, desc, explode, from_unixtime, length, lit, lower, max, regexp_replace, row_number, split, substring, sum, to_json, trim, udf, when}

/**
 * Create with: com.vdian.pickover
 * author: songdongdong
 * E-mail: songdongdong@weidian.com
 * date: 2021/3/2 11:30
 * version: 1.0
 * description: 基于spark的swing召回工程实现
 *
 * https://zhuanlan.zhihu.com/p/67126386
 * https://zhuanlan.zhihu.com/p/143564029
 *
 */
class SwingModel(spark: SparkSession) extends Serializable {

  import spark.implicits._

  var defaultParallelism: Int = spark.sparkContext.defaultParallelism
  var similarities: Option[DataFrame] = None
  var alpha: Option[Double] = Option(0.1)
  var top_n_items: Option[Int] = Option(100) //100

  /**
   * @param parallelism 并行度，不设置，则为spark默认的并行度
   * @return
   */
  def setParallelism(parallelism: Int): SwingModel = {
    this.defaultParallelism = parallelism
    this
  }

  /**
   * @param alpha swing召回模型中的alpha值
   * @return
   */
  def setAlpha(alpha: Double): SwingModel = {
    this.alpha = Option(alpha)
    this
  }

  /**
   * @param top_n_items 计算相似度时，通过count倒排，取前top_n_items个item进行计算
   * @return
   */
  def setTop_N_Items(top_n_items: Int): SwingModel = {
    this.top_n_items = Option(top_n_items)
    this
  }

  /**
   * @param ratings 打分dataset
   * @return
   */
  def fit(ratings: Dataset[User_Query]): SwingModel = {

    case class UserWithItemSet(user_id: String, item_set: Seq[String])

    def interWithAlpha = udf(
      (array_1: Seq[GenericRowWithSchema], array_2: Seq[GenericRowWithSchema]) => {
        var score = 0.0
        val set_1 = array_1.toSet
        val set_2 = array_2.toSet
        val user_set = set_1.intersect(set_2).toArray //intersect 该函数返回两个RDD的交集，并且去重
        for (i <- user_set.indices; j <- i + 1 until user_set.length) {
          val user_1 = user_set(i)
          val user_2 = user_set(j)
          val item_set_1 = user_1.getAs[Seq[String]]("_2").toSet // seq（use_Id, keyword_set),其实这里的 _2 获取的是 keyword_set
          val item_set_2 = user_2.getAs[Seq[String]]("_2").toSet
          score = score + 1 / (item_set_1.intersect(item_set_2).size.toDouble + this.alpha.get)
        }
        score
      }
    )

    val df = ratings.repartition(defaultParallelism) //.cache()
    // 聚合
    val groupUsers = df.groupBy("user_id").agg(collect_set("keyword"))
      .toDF("user_id", "keyword_set").repartition(defaultParallelism)

    //全链接
    val groupItems = df.join(groupUsers, "user_id")
      .rdd.map { x =>
      val keyword = x.getAs[String]("keyword")
      val user_id = x.getAs[String]("user_id")
      val keyword_set = x.getAs[Seq[String]]("keyword_set")

      (keyword, (user_id, keyword_set))

    }
      .toDF("keyword", "user_keyword")
      .groupBy("keyword")
      .agg(collect_set("user_keyword"), count("keyword"))
      .toDF("keyword", "user_keyword_set", "count") // "user_keyword_set" 是很多 [seq1(),seq2(),...]
      .sort($"count".desc)
      .limit(this.top_n_items.get)
      .drop("count")

      .repartition(defaultParallelism)
    //.cache()

    //类似于 join
    var itemJoined: Dataset[Row] = groupItems.join(broadcast(groupItems))
      .toDF("keyword_1", "user_keyword_set_1", "keyword_2", "user_keyword_set_2")
      .filter("keyword_1 <> keyword_2")
      .withColumn("score", interWithAlpha(col("user_keyword_set_1"), col("user_keyword_set_2")))
      .select("keyword_1", "keyword_2", "score") // 这里的分数，还是基于   两个 query 对应的 用户搜索的 keyword相似度进行计算的
      .filter("score > 0")
      .repartition(defaultParallelism)
    //.cache()
    similarities = Option(itemJoined)
    this //返回自己。对象
  }

  /**
   * 从fit结果，对item_id进行聚合并排序，每个item后截取n个item，并返回。
   *
   * @param num 取n个item
   * @return
   */
  def item2item(num: Int): DataFrame = {
    //    case class itemWithScore(item_id: String, score: Double)
    var sim: DataFrame = similarities.get.select("keyword_1", "keyword_2", "score")

    //获取 每个keyword  相似比较高的 keyword
    //    var value: RDD[Row] = sim.rdd.repartition(defaultParallelism)
    val topN:DataFrame = sim.rdd
      .mapPartitions(datas => {
        datas.map { x =>
          val keyword_1 = x.getAs[String]("keyword_1")
          val keyword_2 = x.getAs[String]("keyword_2")
          val score = x.getAs[Double]("score")
          (keyword_1, (keyword_2, score))
        }
      })
      //      .map { x =>
      //          val keyword_1 = x.getAs[String]("keyword_1")
      //          val keyword_2 = x.getAs[String]("keyword_2")
      //          val score = x.getAs[Double]("score")
      //          (keyword_1, (keyword_2, score))
      //      }
      .toDF("keyword", "keywordWithScore")
      .groupBy("keyword").agg(collect_set("keywordWithScore"))
      .toDF("keyword", "keyword_set")
      .rdd.map { x =>
      val item_id_1 = x.getAs[String]("keyword")
      val item_set:String = x.getAs[Seq[GenericRowWithSchema]]("keyword_set")
        .map { x =>
          val item_id_2 = x.getAs[String]("_1")
          val score = x.getAs[Double]("_2")
          (item_id_2, score)
        }.sortBy(-_._2).take(num).map(x => '"' + x._1 + '"').mkString(",")
      (item_id_1, item_set)
    }.toDF("keyword", "sorted_keyword").filter($"sorted_keyword".isNotNull and $"sorted_keyword" =!="")
    //.filter("size(sorted_keyword) > 0")
    topN
  }
}


//用户-搜索-评分
//case class User_Query(user_id:String,keyword:String,rating: Double)
case class User_Query(user_id: String, keyword: String)


//  物品信息
case class Item(item_id: String)

//  用户-物品-评分
//case class Rating(user_id: String, item_id: String, rating: Double)

//  用户信息
case class User(user_id: String)