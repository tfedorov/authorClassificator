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

class ListWordsFreqTest extends AbstractSparkTest {

  @Test
  def testSW(): Unit = {
    val trainRDD = sparkSession.read.text("D:\\work\\workspace\\pet_projects\\authorClassificator\\output\\train\\*").select(input_file_name, col("value"))
      .rdd.map(el => (el.getString(1), (el.get(0).toString.charAt(72).asDigit).toFloat))

    val testRDD = sparkSession.read.text("D:\\work\\workspace\\pet_projects\\authorClassificator\\output\\test\\*").select(input_file_name, col("value"))
      .rdd.map(el => (el.getString(1), (el.get(0).toString.charAt(71).asDigit).toFloat))

    import sparkSession.implicits._
    val trainingDS = sparkSession.createDataset(trainRDD.map(el => Word(el._1, el._2))).as[Word]
    val testDS = sparkSession.createDataset(testRDD.map(el => Word(el._1, el._2))).as[Word]

    val trainGroupDS = trainingDS.rdd.groupBy((_.label)).map { k =>
      val words = k._2.map(_.text).toSeq
      LabelTextCount(k._1, words, words.size)
    }.toDS()
    val testGroupDS = testDS.rdd.groupBy((_.label)).map { k =>
      val words = k._2.map(_.text).toSeq
      LabelTextCount(k._1, words, words.size)
    }.toDS()

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
  def testNew(): Unit = {
    val trainRDD = sparkSession.read.text("D:\\work\\workspace\\pet_projects\\authorClassificator\\output\\raw\\trainRawData*").select(input_file_name, col("value"))
      .rdd.map(file => SentenceLabel(file.getString(1), (file.get(0).toString.charAt(83).asDigit).toFloat))

    val testRDD = sparkSession.read.text("D:\\work\\workspace\\pet_projects\\authorClassificator\\output\\raw\\testRawData*").select(input_file_name, col("value"))
      .rdd.map(file => SentenceLabel(file.getString(1), (file.get(0).toString.charAt(82).asDigit).toFloat))

    import sparkSession.implicits._
    val trainDS = trainRDD.groupBy(_.label).map(_._2.reduce(_ + _)).toDS()
    val testDS = testRDD.groupBy(_.label).map(_._2.reduce(_ + _)).toDS()

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

  @Test
  def testUnion(): Unit = {
    //assertNotNull(null)

    val trainRDD1 = sparkSession.read.text("D:\\work\\workspace\\pet_projects\\authorClassificator\\output\\train\\*").select(input_file_name, col("value"))
      .rdd.map(el => (el.getString(1), (el.get(0).toString.charAt(72).asDigit).toFloat))

    import sparkSession.implicits._
    val trainingDS1 = sparkSession.createDataset(trainRDD1.map(el => Word(el._1, el._2))).as[Word]

    val trainGroupDS1 = trainingDS1.rdd.groupBy((_.label)).map { k =>
      val words = k._2.map(_.text).toSeq
      LabelTextCount(k._1, words, words.size)
    }.toDS()

    val trainRDD2 = sparkSession.read.text("D:\\work\\workspace\\pet_projects\\authorClassificator\\output\\raw\\trainRawData*").select(input_file_name, col("value"))
      .rdd.map(file => SentenceLabel(file.getString(1), (file.get(0).toString.charAt(83).asDigit).toFloat))

    val trainDS2 = trainRDD2.groupBy(_.label).map {
      e =>
        val str = e._2.map(_.sentence).mkString(" ");
        SentenceLabel(str, e._1)
    }.toDS().as[SentenceLabel]

    //val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val regexTokenizer = new RegexTokenizer()
      .setInputCol("sentence")
      .setOutputCol("allText")
      .setPattern("""[ ,.!?№()-\\—\\"_$]""") // alternatively .setPattern("\\w+").setGaps(false)


    //val sWtrans = new ListWordsFreq()
    //sWtrans.transform(trainGroupDS1).foreach(println(_))
    trainGroupDS1.select("label", "allText").foreach(println(_))

    println("***********")
    //sWtrans.transform(r2).foreach(println(_))
    val r2 = regexTokenizer.transform(trainDS2)
    r2.select("label", "allText").foreach(println(_))


  }
}