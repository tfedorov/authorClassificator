package com.tfedorov.aturho.spark.tf

import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
  * Created by Taras_Fedorov on 2/18/2017.
  */
object HashingTFProcessing {
  def apply(trainDF: DataFrame, testRdd: RDD[Iterable[String]]) = {

    val countAllDF = trainDF.groupBy("label").count().toDF("label", "allWords")
    //countAllDF.foreach(println(_))
    //countAllDF.printSchema()

    val hashingTF = new HashingTF()
      .setInputCol("text").setOutputCol("rawFeatures").setNumFeatures(20)

    val featurizedDF = hashingTF.transform(trainDF)

    val gropuedDF = featurizedDF.groupBy("label", "text").count().where("count > 10").sort("text")

    //gropued.foreach(println(_))
    //gropued.printSchema()

    gropuedDF.join(countAllDF, "label").printSchema()
    gropuedDF.join(countAllDF, "label").rdd.map(row => (row.get(0), row.get(1), 1.0 * row.getLong(2) / row.getLong(3))).foreach(println(_))

  }
}
