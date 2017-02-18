package com.tfedorov.aturho.spark

import com.tfedorov.aturho.spark.w2v.Word2VecProcessing
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by Taras_Fedorov on 2/15/2017.
  */
object MLibApp extends App {

  val OUTPUT_FOLDER = "D:\\work\\workspace\\pet_projects\\authorClassificator\\output\\*"


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

  // val pipeline = HashingTFModel()
  //val w2vProcessing = Word2VecProcessing()
  //val hashingTFmodel = pipeline.fit(trainingDF)

  //val word2vecM = w2vProcessing.fit(trainingDF)
  //word2vecM.transform(trainingDF).foreach(println(_))

  // Make predictions on test documents.
  //val prediction = hashingTFmodel.transform(testDF)

  //prediction.collect().foreach(println(_))

}
