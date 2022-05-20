//package com.vdian.pickover.swing

import com.vdian.pickover.SparkOperation
import com.vdian.pickover.swing.{SwingModel, User_Query}
import com.vdian.pickover.run_spark_sql._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{asc, broadcast, col, collect_list, collect_set, concat_ws, count, countDistinct, current_date, datediff, desc, explode, from_unixtime, length, lit, lower, max, regexp_replace, row_number, split, substring, sum, to_json, trim, udf, when}

/**
 * Create with: com.vdian.pickover.swing
 * author: songdongdong
 * E-mail: songdongdong@weidian.com
 * date: 2021/3/2 11:32
 * version: 1.0
 * description: Swing 启动方法
 */
object Swing {

  var time = "test"

  def main(args: Array[String]): Unit = {

    val Array(top_n_items, alpha,num,defaultParallelism, time) = args

    println(top_n_items,alpha,num, defaultParallelism,time) //2000 0.1 16 1000 ${yyyy-MM-dd,-30}   对应 (2000,0.1,,16,1000,2021-02-09)



    val sparkOperation = SparkOperation(this.getClass.getName)
    val spark = sparkOperation.spark
    spark.conf.set("spark.sql.crossJoin.enabled", "true") //开启笛卡尔积
    spark.conf.set("spark.kryoserializer.buffer.max", "80g") //序列缓冲设置下
    //    spark.conf.set("spark.kryoserializer.buffer", "256m") //序列缓冲设置下
    val model  = new SwingModel(spark).setAlpha(alpha.toDouble).setTop_N_Items(top_n_items.toInt)
      .setParallelism(defaultParallelism.toInt)

    import spark.implicits._


    var query_keyword = spark.sql(QUERY_KEYWORD.format(time)).select("user_id","query")
    query_keyword = query_keyword.withColumn("keyword",normalizeChars($"query"))
    query_keyword = query_keyword.select("user_id","keyword")
    var dataset: Dataset[User_Query] = query_keyword.as[User_Query]

    var result: DataFrame = model.fit(dataset).item2item(num.toInt).toDF("keyword", "sorted_keyword")

    val concat_querys = udf((str: String) => "[" + str.trim +"]")
    result = result.withColumn("simaliar_querys", concat_querys($"sorted_keyword"))
    result = result.select("keyword","simaliar_querys")

    sparkOperation.insert_overwrite_table_repartitions("hzsearch.dwd_search_subdivide_simaliar_querys",result,50)


  }

  //只保留中文、数字、英文、大写转小写
  val normalizeChars = udf((text: String) => {
    text.replaceAll("[^\\u4e00-\\u9fa5a-zA-Z0-9]", "").trim //toLowerCase
  })

}
