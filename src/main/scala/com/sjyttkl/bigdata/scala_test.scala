package com.sjyttkl.bigdata

/**
 * Create with: com.sjyttkl.bigdata
 * author: songdongdong
 * E-mail: songdongdong@weidian.com
 * date: 2022/5/20 11:38
 * version: 1.0
 * description: 
 */
object scala_test {
  def first(x: Int)=(y: Int)=> {
    x + y
  }
  def curriedSum(x:Int)(y:Int)=x+y



  def main(args: Array[String]): Unit = {
    var seq = Seq("han", "wo", "hao", "han")
    var f = seq.map((_, 1)).groupBy(_._1).map(x => (x._1, x._2.size))
    println(f.toSeq.sortBy(_._2).reverse)  //ArrayBuffer((han,2), (wo,1), (hao,1))
    val arr = List(List(1, 2, 3), List(3, 4, 5), List(2), List(0))
    println(arr.aggregate(0)(_ + _.reduce(_ + _), _ + _))  //20
    println(arr.map(_.reduce(_ + _))) //  List(6, 12, 2, 0)
    println(arr.map(_.reduce(_ + _)).sum) // 20
    println(arr.map(_.reduce(_ + _)).reduce(_+_))// 20
    //这就是解析
    println(arr.aggregate(10)((x, y) => {
      println(x, y)   //(10,List(1, 2, 3)) 、  (16,List(3, 4, 5)) 、  (28,List(2)) 、  (30,List(0))
      x + y.reduce(_ + _)  // 16 、 28 、30
    }, _ + _))  //30


    println(arr.aggregate(0)(_ + _.reduce(_ + _), _ + _))  //20
    println(arr.aggregate(10)(_ + _.sum, _ + _))  //30

  }
}
