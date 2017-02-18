package com.tfedorov.aturho.spark.w2v

import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
  * Created by Taras_Fedorov on 2/15/2017.
  */
object Word2VecProcessing {
  //val model = new Word2Vec

  def train(trainDF: DataFrame, testRdd: RDD[Iterable[String]]) = {
    println("*****************")
    val firstRDD = getTextForLabel(trainDF, 0)

    val resultFirst = fitW2V(firstRDD)

    printModels(resultFirst)

    val secondRDD = getTextForLabel(trainDF, 0)

    val resultSecond = fitW2V(secondRDD)

    printModels(resultSecond)

    val testModel = fitW2V(testRdd)
    printModels(testModel)
  }

  private def printModels(resultSecond: Word2VecModel) = {
    println("***!!!***bo=" + resultSecond.transform("бо"))
    println("***!!!***scho=" + resultSecond.transform("що"))
  }

  private def fitW2V(testRdd: RDD[Iterable[String]]) = {
    new Word2Vec().setMinCount(1).setVectorSize(3).fit(testRdd)
  }

  private def getTextForLabel(trainDF: DataFrame, label: Float) = {
    trainDF.rdd.map(row => (row.getFloat(1), row.get(0).asInstanceOf[Iterable[String]])).
      reduceByKey(_ ++ _)
      .filter(_._1 == label).values
  }
}
