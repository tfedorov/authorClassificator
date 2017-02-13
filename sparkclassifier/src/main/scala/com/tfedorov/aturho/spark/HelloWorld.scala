package com.tfedorov.aturho.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * Created by Taras_Fedorov on 2/13/2017.
  */
object HelloWorld extends App {

  println("start Hello world app")

  var conf = new SparkConf().setAppName("appName").setMaster("local[*]")
    .set("spark.executor.memory", "1g");
  val sc = new SparkContext(conf)
  val data = Array(1, 2, 3, 4, 5)
  val distData = sc.parallelize(data)

  distData.map(_ * 2).foreach(println(_))

}
