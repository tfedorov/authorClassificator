package com.tfedorov.aturho.spark.model

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.ListWordsFreq

/**
  * Created by Taras_Fedorov on 4/14/2017.
  */
object FreqPipelineBuilder {

  def apply(): Pipeline = {
    val regexTokenizer = new RegexTokenizer()
      .setInputCol("sentence")
      .setOutputCol("allText")
      .setPattern("""[ ,.!?№()-/—\\"_$]""") // alternatively .setPattern("\\w+").setGaps(false)

    val listWordsFreq = new ListWordsFreq()

    val mlr = new LogisticRegression()
      .setMaxIter(1000)
      .setRegParam(0.1)
      .setFeaturesCol("features")
      .setElasticNetParam(0.4)
      .setFamily("multinomial")

    val sqlTrans = new SQLTransformer().setStatement(
      "SELECT *, (label == prediction) AS Diff FROM __THIS__")

    val pipeline = new Pipeline().setStages(Array(regexTokenizer, listWordsFreq, mlr, sqlTrans))
    pipeline
  }

}
