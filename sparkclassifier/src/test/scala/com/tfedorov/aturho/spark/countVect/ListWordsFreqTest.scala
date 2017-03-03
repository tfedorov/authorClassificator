package com.tfedorov.aturho.spark.countVect

import com.tfedorov.aturho.spark.AbstractSparkTest
import com.tfedorov.aturho.spark.tf.Word
import countVect.LabelTextCount
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.RegexTokenizer
import org.apache.spark.ml.linalg.ListWordsFreq
import org.apache.spark.sql.functions.{col, input_file_name}
import org.testng.annotations.Test

/**
  * Created by Taras_Fedorov on 2/23/2017.
  */
case class SentenceLabel(sentence: String, label: Float) {
  def +(newOne: SentenceLabel): SentenceLabel = {
    SentenceLabel(this.sentence + " " + newOne.sentence, this.label)
  }
}

case class SentenceLabelArticle(sentence: String, label: Float, article: String)

case class WordArticle(text: String, label: Float, article: String)

case class LabelTextCountArticle(label: Float, allText: Seq[String], wordsCount: Int, article: String)

class ListWordsFreqTest extends AbstractSparkTest {
  @Test
  def testSW(): Unit = {
    val trainRDD = sparkSession.read.text("D:\\work\\workspace\\pet_projects\\authorClassificator\\output\\train\\*").select(input_file_name, col("value"))
      .rdd.map(el => SentenceLabel(el.getString(1), (el.get(0).toString.charAt(72).asDigit).toFloat))

    val testRDD = sparkSession.read.text("D:\\work\\workspace\\pet_projects\\authorClassificator\\output\\test\\*").select(input_file_name, col("value"))
      .rdd.map(el => WordArticle(el.getString(1), (el.get(0).toString.charAt(71).asDigit).toFloat, el.get(0).toString.substring(71)))

    import sparkSession.implicits._
    val trainingDS = sparkSession.createDataset(trainRDD.map(el => Word(el.sentence, el.label))).as[Word]
    val testDS = sparkSession.createDataset(testRDD).as[WordArticle]

    val trainGroupDS = trainingDS.rdd.groupBy((_.label)).map { k =>
      val words = k._2.map(_.text).toSeq
      LabelTextCount(k._1, words, words.size)
    }.toDS()
    val testGroupDS = testDS.rdd.groupBy((_.article)).map { k =>
      val words = k._2.map(_.text).toSeq
      val label = k._2.head.label
      LabelTextCountArticle(label, words, words.size, k._1)
    }.toDS()
    testGroupDS.show()

    val sWtrans = new ListWordsFreq()

    val mlr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setFeaturesCol("features")
      .setElasticNetParam(0.8)
      .setFamily("multinomial")

    val pipeline = new Pipeline().setStages(Array(sWtrans, mlr))
    val model = pipeline.fit(trainGroupDS)
    val resultDF = model.transform(testGroupDS)
    //resultDF.printSchema()
    trainGroupDS.show()
    resultDF.show()

  }

  @Test
  def testRegexTokenizer(): Unit = {
    val trainRDD = sparkSession.read.text("D:\\work\\workspace\\pet_projects\\authorClassificator\\output\\raw\\trainRawData*").select(input_file_name, col("value"))
      .rdd.map(file => SentenceLabel(file.getString(1), (file.get(0).toString.charAt(83).asDigit).toFloat))

    val testRDD = sparkSession.read.text("D:\\work\\workspace\\pet_projects\\authorClassificator\\output\\raw\\testRawData*").select(input_file_name, col("value"))
      .rdd.map(file => SentenceLabelArticle(file.getString(1), (file.get(0).toString.charAt(82).asDigit).toFloat, file.get(0).toString.substring(82)))

    import sparkSession.implicits._
    val trainDS = trainRDD.groupBy(_.label).map(_._2.reduce(_ + _)).toDS()
    val testDS = testRDD.toDS()

    //val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val regexTokenizer = new RegexTokenizer()
      .setInputCol("sentence")
      .setOutputCol("allText")
      .setPattern("""[ ,.!?№()-\\—\\"_$]""") // alternatively .setPattern("\\w+").setGaps(false)


    val sWtrans = new ListWordsFreq()

    val mlr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setFeaturesCol("features")
      .setElasticNetParam(0.8)
      .setFamily("multinomial")

    val pipeline = new Pipeline().setStages(Array(regexTokenizer, sWtrans, mlr))
    //trainDS.show()
    val model = pipeline.fit(trainDS)
    model.transform(testDS).show()
  }

}