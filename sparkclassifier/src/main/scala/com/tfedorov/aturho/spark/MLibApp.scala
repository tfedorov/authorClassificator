package com.tfedorov.aturho.spark

import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by Taras_Fedorov on 2/15/2017.
  */
object MLibApp extends App {

  private val OUTPUT_FOLDER = "D:\\work\\workspace\\pet_projects\\authorClassificator\\output\\*"


  val sqlContext = SparkSession.builder.
    master("local")
    .appName("spark session example")
    .getOrCreate()


  val sc = sqlContext.sparkContext
  val resRDD = sqlContext.read.text(OUTPUT_FOLDER).select(input_file_name, col("value"))
    .rdd.map(el => (Seq(el.get(1).toString), el.get(0).toString.charAt(66).asDigit - 1 * 1.0))

  //.as[(String, String)] // Optionally convert to Dataset
  // or

  //resRDD.foreach(el => println(el._1))

  val trainingDF = sqlContext.createDataFrame(resRDD).toDF("text", "label")
  val testDF = sqlContext.createDataFrame(Seq((4L, Seq("тому")), (5L, Seq("що")), (6L, Seq("раз")))).toDF("id", "text")

  val pipeline = HashingTFModel()
  val pipeline2 = Word2VecModel()
  val hashingTFmodel = pipeline.fit(trainingDF)

  val word2vecM =  pipeline2.fit(trainingDF)
  word2vecM.transform(trainingDF).foreach(println(_))

  // Make predictions on test documents.
  val prediction = hashingTFmodel.transform(testDF)

  prediction.collect().foreach(println(_))

}
