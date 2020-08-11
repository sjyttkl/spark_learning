package com.sjyttkl.learning;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Create with: com.sjyttkl.learning
 * author: songdongdong
 * E-mail: songdongdong@weidian.com
 * date: 2020/6/7 17:44
 * version: 1.0
 * description:
 */
public class BasicFilter {

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setMaster("local").setAppName(BasicFilter.class.getSimpleName());
        final JavaSparkContext sc = new JavaSparkContext(conf);
        List list = new ArrayList();
        list.add("1");
        list.add("2");
         JavaRDD rdd = sc.parallelize(list);

         rdd = rdd.filter(new Function<String ,Boolean>(){

            @Override
            public Boolean call(String s) throws Exception {
                return s.contains("1");
            }
        });

         System.out.println(rdd);



    }
}


