package com.tfedorov.aturho.spark

import com.tfedorov.aturho.spark.tf.Word
import countVect.CountVectorizerProcessor
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

    //val testRDD = sc.textFile("D:\\work\\workspace\\pet_projects\\authorClassificator\\output\\test\\2_future").map((1, _)).groupByKey().values
    import sparkSession.implicits._
    val trainingDS = sparkSession.createDataset(trainRDD.map(el => Word(el._1, el._2))).as[Word]

    CountVectorizerProcessor(trainingDS)(sparkSession)
  }
}
