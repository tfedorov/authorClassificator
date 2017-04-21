package com.tfedorov.aturho.spark.model

import com.tfedorov.aturho.spark.AbstractSparkTest
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.linalg.{DenseVector, ListWordsFreq, SparseVector}
import org.apache.spark.sql.functions.{col, input_file_name}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.testng.annotations.Test

/**
  * Created by Taras_Fedorov on 2/23/2017.
  */


case class SentenceLabel(sentence: String, label: Float) {
  def +(newOne: SentenceLabel): SentenceLabel = {
    SentenceLabel(this.sentence + " " + newOne.sentence, this.label)
  }
}

class RegexTokenizerTest extends AbstractSparkTest with Serializable {

  @Test
  def testFreq(): Unit = {
    val (trainDS: Dataset[SentenceLabel], testDS: Dataset[SentenceLabel]) = extractTrainTestsDS()

    //import sparkSession.implicits._
    //val trainDSRes: Dataset[SentenceLabel] = trainDS.rdd.map(sentLab => (sentLab.label, sentLab)).reduceByKey(_ + _).map(_._2).toDS
    val trainDSRes = trainDS
    val pipeline: Pipeline = FreqPipelineBuilder()

    val model = pipeline.fit(trainDSRes)

    val trainResults = model.transform(trainDSRes)
    val testResults = model.transform(testDS)

    printResults(trainResults, testResults)

  }


  @Test
  def testCount(): Unit = {
    val (trainDS: Dataset[SentenceLabel], testDS: Dataset[SentenceLabel]) = extractTrainTestsDS()

    //import sparkSession.implicits._
    // val trainDSMerged: Dataset[SentenceLabel] = trainDS.rdd.map(sentLab => (sentLab.label, sentLab)).reduceByKey(_ + _).map(_._2).toDS

    val pipeline: Pipeline = CountPipelineBuilder()

    val model = pipeline.fit(trainDS)

    val trainResults = model.transform(trainDS)
    val testResults = model.transform(testDS)

    printResults(trainResults, testResults)

  }


  private def printResults(trainRes: DataFrame, testRes: DataFrame) = {
    println("***************TRAIN RESULTS******")
    trainRes.show
    println("***************TRAIN FEATURES******")
    trainRes.select("features", "sentence", "Diff").foreach(r => printVector(r.get(0), r.getString(1), r.getBoolean(2)))
    println("***************TEST RESULTS********")
    testRes.show
    println("***************TEST FEATURES******")
    testRes.select("features", "sentence", "Diff").foreach(r => printVector(r.get(0), r.getString(1), r.getBoolean(2)))
    println("***************ESTIMATE******")
    testRes.select("Diff").groupBy("Diff").count().show()
  }

  def printVector(anyVector: Any, sentenceAll: String, diff: Boolean): Unit = {
    val sentence = "\"" + sentenceAll.substring(0, 10) + "\""
    if (anyVector.isInstanceOf[DenseVector])
      return println(sentence + "," + anyVector.asInstanceOf[DenseVector].values.mkString(",") + "," + diff)
    if (anyVector.isInstanceOf[SparseVector])
      return println(sentence + "," + anyVector.asInstanceOf[SparseVector].values.mkString(",") + "," + diff)
    println(sentence + "," + anyVector + "," + diff)

  }

  private def extractTrainTestsDS() = {
    val trainRDD = sparkSession.read.text("../output/raw/trainRawData*").select(input_file_name, col("value"))
      .rdd.map(file => SentenceLabel(textFromFile(file), labelFromFile(file, 83)))

    val testRDD = sparkSession.read.text("../output/raw/testRawData*").select(input_file_name, col("value"))
      .rdd.map(file => SentenceLabel(textFromFile(file), labelFromFile(file, 82)))

    import sparkSession.implicits._
    val trainDS = trainRDD.toDS()
    val testDS = testRDD.toDS()
    (trainDS, testDS)
  }

  private def textFromFile(file: Row) = {
    file.getString(1).toLowerCase + ListWordsFreq.STOP_WORDS
  }

  private def labelFromFile(file: Row, labelPosition: Int) = {
    (file.get(0).toString.charAt(labelPosition).asDigit).toFloat
  }
}