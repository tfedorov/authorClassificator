package com.tfedorov.aturho.spark

import com.tfedorov.aturho.spark.model.FreqPipelineBuilder
import org.apache.spark.SparkContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.linalg.ListWordsFreq
import org.apache.spark.sql.functions.{col, input_file_name}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
  * Created by Taras_Fedorov on 2/15/2017.
  */
object MLibApp extends App {

  case class SentenceLabel(sentence: String, label: Float) {
    def +(newOne: SentenceLabel): SentenceLabel = {
      SentenceLabel(this.sentence + " " + newOne.sentence, this.label)
    }
  }

  val sparkSession: SparkSession = SparkSession.builder.
    master("local")
    .appName("spark test")
    .getOrCreate()
  val sc: SparkContext = sparkSession.sparkContext

  val (trainDS: Dataset[SentenceLabel], testDS: Dataset[SentenceLabel]) = extractTrainTestsDS()

  val pipeline: Pipeline = FreqPipelineBuilder()

  val model = pipeline.fit(trainDS)

  val trainResults = model.transform(trainDS)
  val testResults = model.transform(testDS)

  trainResults.show()
  testResults.show()

  private def extractTrainTestsDS() = {
    val trainRDD = sparkSession.read.text("output/raw/trainRawData*").select(input_file_name, col("value"))
      .rdd.map(file => SentenceLabel(textFromFile(file), labelFromFile(file, 83)))

    val testRDD = sparkSession.read.text("output/raw/testRawData*").select(input_file_name, col("value"))
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
