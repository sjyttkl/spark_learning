/**
  * Create with:  
  * author: songdongdong  
  * E-mail: songdongdong@jd.com 
  * date: 2019/8/14 15:18 
  * version: 1.0
  * description:  
  */
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkWordCountWithScala {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()

    /**
      * 如果这个参数不设置，默认认为你运行的是集群模式
      * 如果设置成local代表运行的是local模式
      */
    conf.setMaster("local")
    //设置任务名
    conf.setAppName("WordCount")
    //创建SparkCore的程序入口
    val sc = new SparkContext(conf)
    //读取文件 生成RDD
    val file: RDD[String] = sc.textFile("D:\\hello.txt")
    //把每一行数据按照，分割
    val word: RDD[String] = file.flatMap(_.split(","))
    //让每一个单词都出现一次
    val wordOne: RDD[(String, Int)] = word.map((_, 1))
    //单词计数
    val wordCount: RDD[(String, Int)] = wordOne.reduceByKey(_ + _)
    //按照单词出现的次数 降序排序
    val sortRdd: RDD[(String, Int)] = wordCount.sortBy(tuple => tuple._2, false)
    //将最终的结果进行保存
    sortRdd.saveAsTextFile("E:\\result")

    sc.stop()
  }
}