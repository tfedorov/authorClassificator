package com.tfedorov.aturho.spark

import com.tfedorov.aturho.spark.tf.HashingTFProcessing
import com.tfedorov.aturho.spark.w2v.Word2VecProcessing
import org.apache.spark.sql.functions.{col, input_file_name}
import org.testng.annotations.Test

/**
  * Created by Taras_Fedorov on 2/17/2017.
  */
class Word2VecProcessingTest extends AbstractSparkTest {


  @Test(enabled = false)
  def smokeTests(): Unit = {
    val traindDF = sqlContext.createDataFrame(Seq(("aaa", 0.0.toFloat))).toDF
    // Word2VecProcessing.train(traindDF)
  }


  @Test
  def testInputW2V(): Unit = {

    val trainRDD = sqlContext.read.text("D:\\work\\workspace\\pet_projects\\authorClassificator\\output\\train\\*").select(input_file_name, col("value"))
      .rdd.map(el => (Seq(el.get(1).toString), (el.get(0).toString.charAt(72).asDigit - 1 * 1.0).toFloat))

    val testRDD = sc.textFile("D:\\work\\workspace\\pet_projects\\authorClassificator\\output\\test\\2_future").map((1, _)).groupByKey().values

    val trainingDF = sqlContext.createDataFrame(trainRDD).toDF("text", "label")
    Word2VecProcessing.train(trainingDF, testRDD)
  }

}
