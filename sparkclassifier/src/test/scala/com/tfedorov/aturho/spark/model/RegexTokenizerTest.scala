package com.tfedorov.aturho.spark.model

import com.tfedorov.aturho.spark.AbstractSparkTest
import org.apache.spark.ml.Pipeline
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

    import sparkSession.implicits._
    val trainDSMerged: Dataset[SentenceLabel] = trainDS.rdd.map(sentLab => (sentLab.label, sentLab)).reduceByKey(_ + _).map(_._2).toDS
    val pipeline: Pipeline = FreqPipelineBuilder()

    val model = pipeline.fit(trainDS)

    val trainResults = model.transform(trainDS)
    val testResults = model.transform(testDS)

    printResults(trainResults, testResults)

  }


  @Test
  def testCount(): Unit = {
    val (trainDS: Dataset[SentenceLabel], testDS: Dataset[SentenceLabel]) = extractTrainTestsDS()

    val pipeline: Pipeline = CountPipelineBuilder()

    val model = pipeline.fit(trainDS)

    val trainResults = model.transform(trainDS)
    val testResults = model.transform(testDS)

    printResults(trainResults, testResults)

  }


  private def printResults(trainRes: DataFrame, testRes: DataFrame) = {
    println("***************TRAIN RESULTS******")
    trainRes.show
    //trainRes.select("features").foreach(r => println(r.get(0).asInstanceOf[SparseVector].values.mkString(",")))
    println("***********************************\n")
    println("***************TEST RESULTS********")
    //testRes.select("features").foreach(r => println(r.get(0).asInstanceOf[SparseVector].values.mkString(",")))
    testRes.show
    println("***********************************")
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
    file.getString(1).toLowerCase
  }

  private def labelFromFile(file: Row, labelPosition: Int) = {
    (file.get(0).toString.charAt(labelPosition).asDigit).toFloat
  }
}