package com.tfedorov.aturho.spark.tf

import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

/**
  * Created by Taras_Fedorov on 2/18/2017.
  */
object TermFrequencyProcessin {
  def apply(trainDF: DataFrame, testRdd: RDD[Iterable[String]]) = {

    val countAllDF = trainDF.groupBy("label").count().toDF("label", "allWords")
    //countAllDF.foreach(println(_))
    //countAllDF.printSchema()

    val gropuedDF = trainDF.groupBy("label", "text").count().where("count > 10").sort("text")

    //gropued.foreach(println(_))
    //gropued.printSchema()

    //gropuedDF.join(countAllDF, "label").printSchema()
    val calculatedRDD = gropuedDF.join(countAllDF, "label").rdd.map(
      row => (row.get(1).asInstanceOf[mutable.WrappedArray[String]].head, 1.0 * row.getLong(2) / row.getLong(3)))
    calculatedRDD.groupBy(_._1).values.filter(_.size == 3)  .foreach(el => println(el.head._1))
    //calculatedRDD.foreach(println(_))
  }
}
