package com.tfedorov.aturho.spark

import com.tfedorov.aturho.spark.tf.{TermFrequencyProcessin, Word}
import org.apache.spark.sql.functions.{col, input_file_name}
import org.testng.annotations.Test

/**
  * Created by Taras_Fedorov on 2/20/2017.
  */
class TermFreqTest extends AbstractSparkTest {

  import sparkSession.implicits._

  @Test
  def testTF(): Unit = {

    val trainRDD = sparkSession.read.text("D:\\work\\workspace\\pet_projects\\authorClassificator\\output\\train\\*").select(input_file_name, col("value"))
      .rdd.map(el => (el.getString(1), (el.get(0).toString.charAt(72).asDigit).toFloat))

    //val testRDD = sc.textFile("D:\\work\\workspace\\pet_projects\\authorClassificator\\output\\test\\2_future").map((1, _)).groupByKey().values

    val trainingDF = sparkSession.createDataset(trainRDD.map(el => Word(el._1, el._2))).as[Word]

    TermFrequencyProcessin(trainingDF)(sparkSession)
  }
}
