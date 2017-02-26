package com.tfedorov.aturho.spark.countVect

import com.tfedorov.aturho.spark.AbstractSparkTest
import com.tfedorov.aturho.spark.tf.Word
import countVect.{CountVectorizerProcessor, LabelTextCount, StopWordCountTranformer}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.SWTransformer
import org.apache.spark.sql.functions.{col, input_file_name}
import org.testng.annotations.Test

/**
  * Created by Taras_Fedorov on 2/23/2017.
  */
class CountVectorizerTest extends AbstractSparkTest {


  @Test
  def testCountVectSimple(): Unit = {
    val trainRDD = sparkSession.read.text("D:\\work\\workspace\\pet_projects\\authorClassificator\\output\\train\\*").select(input_file_name, col("value"))
      .rdd.map(el => (el.getString(1), (el.get(0).toString.charAt(72).asDigit).toFloat))

    val testRDD = sparkSession.read.text("D:\\work\\workspace\\pet_projects\\authorClassificator\\output\\test\\*").select(input_file_name, col("value"))
      .rdd.map(el => (el.getString(1), (el.get(0).toString.charAt(71).asDigit).toFloat))

    import sparkSession.implicits._
    val trainingDS = sparkSession.createDataset(trainRDD.map(el => Word(el._1, el._2))).as[Word]
    val testDS = sparkSession.createDataset(testRDD.map(el => Word(el._1, el._2))).as[Word]

    CountVectorizerProcessor(trainingDS, testDS)(sparkSession)
  }

  @Test
  def testCountVectPipeline(): Unit = {
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

    StopWordCountTranformer(trainGroupDS)(sparkSession).show()
  }

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
    val sWtrans = new SWTransformer()

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
    resultDF.show()

  }
}