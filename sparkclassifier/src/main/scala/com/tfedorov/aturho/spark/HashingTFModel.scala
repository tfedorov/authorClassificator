package com.tfedorov.aturho.spark

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.HashingTF

/**
  * Created by Taras_Fedorov on 2/15/2017.
  */
object HashingTFModel {

  def apply() = {
    val hashingTF = new HashingTF()
      .setNumFeatures(100)
      .setInputCol("text")
      .setOutputCol("features")
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.001)
    new Pipeline()
      .setStages(Array(hashingTF, lr))
  }

}
