package com.tfedorov.aturho.spark.countVect

import com.tfedorov.aturho.spark.AbstractSparkTest
import com.tfedorov.aturho.spark.tf.Word
import countVect.{CountVectorizerProcessor, LabelTextCount, StopWordCountTranformer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, input_file_name}
import org.testng.annotations.Test

/**
  * Created by Taras_Fedorov on 2/23/2017.
  */
class CountVectorizerTest extends AbstractSparkTest {


  @Test
  def testCountVectSimple(): Unit = {
    val (trainRDD, testRDD) = prepareTrainTest

    import sparkSession.implicits._
    val trainingDS = sparkSession.createDataset(trainRDD.map(el => Word(el._1, el._2))).as[Word]
    val testDS = sparkSession.createDataset(testRDD.map(el => Word(el._1, el._2))).as[Word]

    CountVectorizerProcessor(trainingDS, testDS)(sparkSession)
  }

  @Test
  def testCountVectPipeline(): Unit = {
    val (trainRDD, testRDD) = prepareTrainTest

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


}