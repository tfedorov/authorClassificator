package org.apache.spark.ml.linalg

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util._
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Created by Taras_Fedorov on 2/25/2017.
  */
object ListWordsFreq {

  val LIST_WORDS_DEFAULT = Seq("від", "навіть", "про", "які", "до", "та",
    "як", "із", "що", "під", "на", "не", "для", "за", "тому", "це")
}

class ListWordsFreq(stopWords: Seq[String] = ListWordsFreq.LIST_WORDS_DEFAULT) extends Transformer with HasInputCol with HasOutputCol with DefaultParamsWritable {

  val LIST_WORDS_LABEL_TERMS = Seq(LabelTextCount(1.0.toFloat, stopWords, stopWords.size))

  val sparkSession = SparkSession.builder().getOrCreate()

  import sparkSession.implicits._

  val cvModel: CountVectorizerModel = new CountVectorizer()
    .setInputCol("allText")
    .setOutputCol("features")
    .fit(LIST_WORDS_LABEL_TERMS.toDS())


  override def transform(dataset: Dataset[_]): DataFrame = {

    val extendedTextDF = dataset.toDF().map { row =>
      val label = row.getAs[Float]("label")
      val extendedText = stopWords ++ row.getAs[Seq[String]]("allText")
      LabelTextCount(label, extendedText, extendedText.size)
    }

    cvModel.transform(extendedTextDF)
  }

  override def copy(extra: ParamMap): Transformer = ???

  override def transformSchema(schema: StructType): StructType = {
    SchemaUtils.appendColumn(schema, "features", new VectorUDT(), false)
  }

  override val uid: String = "1234"

}
