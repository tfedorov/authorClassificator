package com.tfedorov.aturho.spark

import com.tfedorov.aturho.spark.tf.Word
import countVect.CountVectorizerProcessor
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{col, input_file_name}
import org.testng.annotations.Test

/**
  * Created by Taras_Fedorov on 2/23/2017.
  */
class CountVectorizerTest extends AbstractSparkTest {


  @Test
  def testTF(): Unit = {
    val trainRDD = sparkSession.read.text("D:\\work\\workspace\\pet_projects\\authorClassificator\\output\\train\\*").select(input_file_name, col("value"))
      .rdd.map(el => (el.getString(1), (el.get(0).toString.charAt(72).asDigit).toFloat))

    val testRDD = sparkSession.read.text("D:\\work\\workspace\\pet_projects\\authorClassificator\\output\\test\\*").select(input_file_name, col("value"))
      .rdd.map(el => (el.getString(1), (el.get(0).toString.charAt(71).asDigit).toFloat))

    import sparkSession.implicits._
    val trainingDS = sparkSession.createDataset(trainRDD.map(el => Word(el._1, el._2))).as[Word]
    val testDS = sparkSession.createDataset(testRDD.map(el => Word(el._1, el._2))).as[Word]

    CountVectorizerProcessor(trainingDS, testDS)(sparkSession)
  }
}
