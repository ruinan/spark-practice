package com.ruinan.spark;

import org.apache.spark.sql.SparkSession;

public class MySpark {
  SparkSession spark;

  public void init() {
    // spark 2.0 初始化一个spark session， 本地的
    if (spark == null) {
      spark = SparkSession.builder().appName("RuiApplication").master("local").getOrCreate();
    }
  }

  public void stop() {
    spark.stop();
  }

}
