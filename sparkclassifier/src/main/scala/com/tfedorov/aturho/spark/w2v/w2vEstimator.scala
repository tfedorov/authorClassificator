package com.tfedorov.aturho.spark.w2v

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.Word2Vec

/**
  * Created by Taras_Fedorov on 2/17/2017.
  */
object w2vEstimator {
  def apply() = {
    val word2vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(2)
      .setMinCount(0)

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.001)

    //val st = new WordsVec

    new Pipeline()
      .setStages(Array(word2vec, lr))
  }

}
