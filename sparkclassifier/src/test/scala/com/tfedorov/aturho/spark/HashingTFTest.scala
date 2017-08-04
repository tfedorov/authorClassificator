package com.tfedorov.aturho.spark

import com.tfedorov.aturho.spark.tf.{HashingTFProcessing, TermFrequencyProcessin}
import org.apache.spark.sql.functions.{col, input_file_name}
import org.testng.annotations.Test;

/**
  * Created by Taras_Fedorov on 2/18/2017.
  */
class HashingTFTest extends AbstractSparkTest {

  @Test
  def testHashingTF(): Unit = {


    val trainRDD = sparkSession.read.text("C:\\work\\workspace\\private\\authorClassificator\\output\\train\\*").select(input_file_name, col("value"))
      .rdd.map(el => (Seq(el.get(1).toString), (el.get(0).toString.charAt(69).asDigit).toFloat))

    val testRDD = sc.textFile("C:\\work\\workspace\\private\\authorClassificator\\output\\test\\2_future").map((1, _)).groupByKey().values

    val trainingDF = sparkSession.createDataFrame(trainRDD).toDF("text", "label")

    HashingTFProcessing(trainingDF, testRDD)
  }


}
