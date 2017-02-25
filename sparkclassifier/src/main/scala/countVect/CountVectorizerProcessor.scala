package countVect

import com.tfedorov.aturho.spark.tf.Word
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Created by Taras_Fedorov on 2/23/2017.
  */
object CountVectorizerProcessor {

  val wordsDeterm = Seq("від", "навіть", "про", "які", "до", "та", "як", "із", "що", "під", "на", "не", "для", "за", "тому", "це")

  def apply(trainDS: Dataset[Word], testDS: Dataset[Word])(implicit sparkSession: SparkSession) = {
    import sparkSession.implicits._

    val trainGroupDS = trainDS.rdd.groupBy((_.label)).map { k =>
      val words = wordsDeterm ++ k._2.map(_.text).toSeq
      LabelTextCount(k._1, words, words.size)
    }.toDS()


    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("allText")
      .setOutputCol("features")
      .fit(Seq(LabelTextCount(1.0.toFloat, wordsDeterm, wordsDeterm.size)).toDS())

    val cvTrainDS = cvModel.transform(trainGroupDS)

    val trainLabelFeatureDS = cvTrainDS.map {
      row =>
        val label = row.getFloat(0)
        val featureVector = row.get(3).asInstanceOf[SparseVector]
        val oneWordFrequency: Double = 1.0 / row.getInt(2)
        LabelFeature(label, featureVector) * oneWordFrequency
    }


    val mlr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
      .setFamily("multinomial")

    val mlrModel = mlr.fit(trainLabelFeatureDS)

    val testGroupDS = testDS.rdd.groupBy((_.label)).map { k =>
      val words = wordsDeterm ++ k._2.map(_.text).toSeq
      LabelTextCount(k._1, words, words.size)
    }.toDS().as[LabelTextCount]

    val cvTestDS = cvModel.transform(testGroupDS)

    val testLabelFeatureDS = cvTestDS.map { row =>
      val label = row.getFloat(0)
      val featureVector = row.get(3).asInstanceOf[SparseVector]
      val oneWordFrequency: Double = 1.0 / row.getInt(2)
      LabelFeature(label, featureVector) * oneWordFrequency
    }

    mlrModel.transform(testLabelFeatureDS).show(true)

  }

}
