package com.ruinan.spark;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class MySpark {
  SparkSession sparkSession;
  SparkContext sparkContext;

  public void init() {
    // spark 2.0 初始化一个spark session， 本地的
    if (sparkSession == null) {
      sparkSession = SparkSession.builder().appName("RuiApplication").master("local").getOrCreate();
      sparkContext = sparkSession.sparkContext();
    }

  }

  public void stop() {
    sparkSession.stop();
  }

  public void read() {
    // 读取csv文件
    JavaRDD<String> lines1 = sparkContext.textFile("src/main/resources/text1.txt", 3).toJavaRDD();
    JavaRDD<String> words = lines1.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
    JavaPairRDD<String, Iterable<String>> stringIterableJavaPairRDD = words.groupBy(s -> s);
    JavaRDD<String> map = stringIterableJavaPairRDD.map(s -> s._1 + " " + ((Collection<String>) s._2).size());
    List<String> collect = map.collect();
    collect.forEach(System.out::println);

  }
}
