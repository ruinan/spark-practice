package com.ruinan.spark;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;

public class Main {

  public MySpark mySpark;

  public static void main(String[] args) {
    MySpark mySpark = new MySpark();
    mySpark.init();
    mySpark.read();
    mySpark.stop();
  }
}
