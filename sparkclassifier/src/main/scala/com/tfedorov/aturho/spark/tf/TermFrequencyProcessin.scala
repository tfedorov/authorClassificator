package com.tfedorov.aturho.spark.tf

import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Created by Taras_Fedorov on 2/18/2017.
  */
object TermFrequencyProcessin {

  val wordsDeterm = Seq("від", "навіть", "про", "які", "до", "та", "як", "із", "що", "під", "на", "не", "для", "за", "тому", "україни", "це")

  case class LabelAllWords(label: Float, count: Long)

  case class LabelWordCount(label: Float, text: String, count: Long)

  case class LabelWordFrequency(label: Float, text: String, freq: Float)

  def apply(trainDS: Dataset[Word])(implicit sparkSession: SparkSession) = {
    import sparkSession.implicits._

    val wordsCount = trainDS.groupBy("label").count().as[LabelAllWords]
    //wordsCount.printSchema()

    val determCount = trainDS.filter(word => wordsDeterm.contains(word.text)).groupBy("label", "text").count().as[LabelWordCount]
    //determCount.printSchema()


    determCount.as("L").joinWith(wordsCount.as("R"), $"L.label" === $"R.label").map(buildLabelWordFreq(_)).rdd.groupBy(_.label).foreach(println(_))
  }

  private def buildLabelWordFreq(joined: (LabelWordCount, LabelAllWords)) = {
    val frequency: Double = 1.0 * joined._1.count / joined._2.count
    LabelWordFrequency(joined._1.label, joined._1.text, frequency.toFloat)
  }
}
