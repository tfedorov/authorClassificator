package countVect

import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Created by Taras_Fedorov on 2/25/2017.
  */
object StopWordCountTranformer {


  val STOP_WORDS = Seq("від", "навіть", "про", "які", "до", "та", "як", "із", "що", "під", "на", "не", "для", "за", "тому", "це")
  val STOP_WORDS_LABEL_TERMS = Seq(LabelTextCount(1.0.toFloat, STOP_WORDS, STOP_WORDS.size))

  def apply(input: Dataset[LabelTextCount])(implicit sparkSession: SparkSession): Dataset[LabelFeature] = {
    import sparkSession.implicits._

    val inpPlusStopWordDS = input.map(input => LabelTextCount(input.label, STOP_WORDS ++ input.allText, input.wordsCount))

    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("allText")
      .setOutputCol("features")
      .fit(STOP_WORDS_LABEL_TERMS.toDS())

    cvModel.transform(inpPlusStopWordDS).map { row =>
      val label = row.getFloat(0)
      val featureVector = row.get(3).asInstanceOf[SparseVector]
      val oneWordFrequency: Double = 1.0 / row.getInt(2)
      LabelFeature(label, featureVector) * oneWordFrequency
    }.as[LabelFeature]
  }
}
