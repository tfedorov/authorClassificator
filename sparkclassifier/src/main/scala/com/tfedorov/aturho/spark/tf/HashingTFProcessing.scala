package com.tfedorov.aturho.spark.tf

import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions

/**
  * Created by Taras_Fedorov on 2/18/2017.
  */
object HashingTFProcessing {
  def apply(trainDF: DataFrame, testRdd: RDD[Iterable[String]]) = {

    trainDF.groupBy("label").count().foreach(println(_))

    val hashingTF = new HashingTF()
      .setInputCol("text").setOutputCol("rawFeatures").setNumFeatures(20)

    val featurizedDF = hashingTF.transform(trainDF)

    val gropued = featurizedDF.groupBy("label", "text").count().sort()

    gropued.printSchema()
    gropued.where("count > 2").sort("count").foreach(println(_))

  }
}
