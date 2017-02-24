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

  val wordsDeterm = Seq("від", "навіть", "про", "які", "до", "та", "як", "із", "що", "під", "на", "не", "для", "за", "тому", "україни", "це")

  def apply(trainDS: Dataset[Word], testRDD: RDD[Iterable[String]])(implicit sparkSession: SparkSession) = {
    import sparkSession.implicits._

    val stopWordsDS = trainDS.rdd.groupBy((_.label)).map { k =>
      val words = k._2.map(_.text).toSeq
      LabelText(k._1, words, words.size)
    }.toDS()

    //trainDS.rdd.groupBy((_.label)).map(k => LabelText(k._1, k._2.map(_.text).toSeq)).foreach(println(_))

    //    val trainedDS = trainDS.map(w => LabelText(w.label, Seq(w.text)))

    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("allText")
      .setOutputCol("features")
      .fit(Seq(LabelText(1.0.toFloat, wordsDeterm, wordsDeterm.size)).toDS())

    val resultDS = cvModel.transform(stopWordsDS)

    val addDS = resultDS.map {
      r => (r.getFloat(0).toInt, Vectors.dense(r.get(3).asInstanceOf[SparseVector].values.map(_ * 1.0 / r.getInt(2))))
    }.toDF("label", "features")


    val mlr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
      .setFamily("multinomial")

    val mlrModel = mlr.fit(addDS)

    mlrModel.transform(addDS).show(true)

    val resultTestDS = cvModel.transform(
      testRDD.map(e => (1.0, e.toSeq, e.toSeq.size)).toDF("label", "allText", "count"))
    resultTestDS.show(true)

    val addTestDS = resultTestDS.map {
      r => (r.getDouble(0).toInt, Vectors.dense(r.get(3).asInstanceOf[SparseVector].values.map(_ * 1.0 / r.getInt(2))))
    }.toDF("label", "features")
    mlrModel.transform(addTestDS).show(true)

  }
}
