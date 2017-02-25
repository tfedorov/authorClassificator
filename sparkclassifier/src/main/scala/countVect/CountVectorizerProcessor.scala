package countVect

import com.tfedorov.aturho.spark.tf.Word
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.{SparseVector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Created by Taras_Fedorov on 2/23/2017.
  */
object CountVectorizerProcessor {

  case class LabelText(label: Float, allText: Seq[String], wordsCount: Int)

  val wordsDeterm = Seq("від", "навіть", "про", "які", "до", "та", "як", "із", "що", "під", "на", "не", "для", "за", "тому", "це")

  def apply(trainDS: Dataset[Word], testDS: Dataset[Word])(implicit sparkSession: SparkSession) = {
    import sparkSession.implicits._

    val trainGroupDS = trainDS.rdd.groupBy((_.label)).map { k =>
      //val words = wordsDeterm ++ k._2.map(_.text).toSeq
      val words = wordsDeterm ++ k._2.map(_.text).toSeq
      LabelText(k._1, words, words.size)
    }.toDS().as[LabelText]


    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("allText")
      .setOutputCol("features")
      .fit(Seq(LabelText(1.0.toFloat, wordsDeterm, wordsDeterm.size)).toDS())

    val cvTrainDS = cvModel.transform(trainGroupDS)

    val trainLabelFeatureDS = cvTrainDS.map {
      r => (r.getFloat(0).toInt, vectorMultiple(r.get(3).asInstanceOf[SparseVector], 1.0 / r.getInt(2)))
    }.toDF("label", "features")


    val mlr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
      .setFamily("multinomial")

    val mlrModel = mlr.fit(trainLabelFeatureDS)

    val testGroupDS = testDS.rdd.groupBy((_.label)).map { k =>
      val words = wordsDeterm ++ k._2.map(_.text).toSeq
      LabelText(k._1, words, words.size)
    }.toDS().as[LabelText]

    val cvTestDS = cvModel.transform(testGroupDS)

    val testLabelFeatureDS = cvTestDS.map {
      r => (r.getFloat(0).toInt, vectorMultiple(r.get(3).asInstanceOf[SparseVector], 1.0 / r.getInt(2)))
    }.toDF("label", "features")
    /*
        println("testDS.show()")
        testDS.show()
        println("testGroupDS")
        testGroupDS.show()
        println("cvTestDS")
        cvTestDS.show()
        println("testLabelFeatureDS")
        testLabelFeatureDS.show()
        println(mlrModel.explainParams())
    */

    mlrModel.transform(testLabelFeatureDS).show(true)

  }

  private def vectorMultiple(vector: SparseVector, multiPlicator: Double) = {
    Vectors.dense(vector.values.map(_ * multiPlicator)).toSparse
  }
}
