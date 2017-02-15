package com.tfedorov.aturho.spark

import org.apache.spark.ml.feature.Word2Vec

/**
  * Created by Taras_Fedorov on 2/15/2017.
  */
object Word2VecModel {
  def apply() = {
    new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(2)
      .setMinCount(0)
  }
}
