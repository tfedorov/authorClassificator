package com.tfedorov.aturho.spark

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.testng.Assert

/**
  * Created by Taras_Fedorov on 2/17/2017.
  */
class AbstractSparkTest extends Assert {

  val sparkSession: SparkSession = SparkSession.builder.
    master("local")
    .appName("spark test")
    .getOrCreate()
  val sc: SparkContext = sparkSession.sparkContext
  
  protected def prepareTrainTest: (RDD[(String, Float)], RDD[(String, Float)]) = {
    val trainRDD = sparkSession.read.text("C:\\work\\workspace\\private\\authorClassificator\\output\\train\\*").select(input_file_name, col("value"))
      .rdd.map(el => (el.getString(1), (el.get(0).toString.charAt(69).asDigit).toFloat))

    val testRDD = sparkSession.read.text("C:\\work\\workspace\\private\\authorClassificator\\output\\test\\*").select(input_file_name, col("value"))
      .rdd.map(el => (el.getString(1), (el.get(0).toString.charAt(70).asDigit).toFloat))
    (trainRDD, testRDD)
  }


}
