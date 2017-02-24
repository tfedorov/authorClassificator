package countVect

import com.tfedorov.aturho.spark.tf.Word
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.{SparseVector, Vectors}
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Created by Taras_Fedorov on 2/23/2017.
  */
object CountVectorizerProcessor {

  case class LabelText(label: Float, allText: Seq[String])

  val wordsDeterm = Seq("від", "навіть", "про", "які", "до", "та", "як", "із", "що", "під", "на", "не", "для", "за", "тому", "україни", "це")


  def apply(trainDS: Dataset[Word])(implicit sparkSession: SparkSession) = {
    import sparkSession.implicits._

    val trainedDS = trainDS.rdd.groupBy((_.label)).map(k => LabelText(k._1, k._2.map(_.text).toSeq)).toDS()

    //trainDS.rdd.groupBy((_.label)).map(k => LabelText(k._1, k._2.map(_.text).toSeq)).foreach(println(_))

    //    val trainedDS = trainDS.map(w => LabelText(w.label, Seq(w.text)))

    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("allText")
      .setOutputCol("features")
      .fit(Seq(LabelText(1.0.toFloat, wordsDeterm)).toDS())

    val resultDS = cvModel.transform(trainedDS)
    //resultDS.show(false)

    val addDS = resultDS.map { r => (r.getFloat(0).toInt, r.get(2).asInstanceOf[SparseVector].values.map(_ * 0.5), Vectors.dense(35.0)) }.toDF("id", "features", "vec2")

    addDS.printSchema()
    addDS.show(false)


    //interaction.transform(addDS).show()
  }
}
