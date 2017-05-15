package com.tfedorov.aturho.spark.model

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{RegexTokenizer, SQLTransformer}
import org.apache.spark.ml.linalg.ListWordsCount

/**
  * Created by Taras_Fedorov on 4/14/2017.
  */
object CountPipelineBuilder {
  def apply() = {
    val regexTokenizer = new RegexTokenizer()
      .setInputCol("sentence")
      .setOutputCol("allText")
      .setPattern("""[ ,.!?№()-/—\\"_$]""") // alternatively .setPattern("\\w+").setGaps(false)

    val listWordsCount = new ListWordsCount()

    val mlr = new LogisticRegression()
      .setMaxIter(1000)
      .setRegParam(0.1)
      .setFeaturesCol("features")
      .setElasticNetParam(0.01)
      .setFamily("multinomial")

    val sqlTrans = new SQLTransformer().setStatement(
      "SELECT *, (label == prediction) AS Diff FROM __THIS__")

    val pipeline = new Pipeline().setStages(Array(regexTokenizer, listWordsCount, mlr, sqlTrans))
    pipeline
  }

}
