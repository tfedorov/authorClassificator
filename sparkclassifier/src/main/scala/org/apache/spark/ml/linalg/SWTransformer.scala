package org.apache.spark.ml.linalg

import countVect.{LabelFeature, LabelTextCount}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util._
import org.apache.spark.sql.catalyst.expressions.{GenericRow, GenericRowWithSchema}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Created by Taras_Fedorov on 2/25/2017.
  */
class SWTransformer(sparkSession: SparkSession) extends Transformer with DefaultParamsWritable {

  val STOP_WORDS = Seq("від", "навіть", "про", "які", "до", "та", "як", "із", "що", "під", "на", "не", "для", "за", "тому", "це")
  val STOP_WORDS_LABEL_TERMS = Seq(LabelTextCount(1.0.toFloat, STOP_WORDS, STOP_WORDS.size))

  import sparkSession.implicits._

  val cvModel: CountVectorizerModel = new CountVectorizer()
    .setInputCol("allText")
    .setOutputCol("features")
    .fit(STOP_WORDS_LABEL_TERMS.toDS())


  override def transform(dataset: Dataset[_]): DataFrame = {
    import sparkSession.implicits._
    val inpPlusStopWordDS = dataset.map { labTextCnt =>
      LabelTextCount(labTextCnt.label, STOP_WORDS ++ labTextCnt.allText, labTextCnt.wordsCount)
    }
    cvModel.transform(inpPlusStopWordDS).map { row =>
      val label = row.getFloat(0)
      val featureVector = row.get(3).asInstanceOf[SparseVector]
      val oneWordFrequency: Double = 1.0 / row.getInt(2)
      LabelFeature(label, featureVector) * oneWordFrequency
    }.toDF("label", "features")
  }

  implicit def any2LabelTextCount(any: Any): LabelTextCount = {
    if (any.isInstanceOf[LabelTextCount])
      return any.asInstanceOf[LabelTextCount]

    val genericRow = any.asInstanceOf[GenericRow]
    val seq = genericRow.getSeq[String](1)
    LabelTextCount(genericRow.getFloat(0), seq, seq.size)

  }

  override def copy(extra: ParamMap): Transformer = ???

  override def transformSchema(schema: StructType): StructType = {

    //val inputType = schema("allText").dataType
    //require(inputType.sameType(ArrayType(StringType)),
    //  s"Input type must be ArrayType(StringType) but got $inputType.")
    //SchemaUtils.appendColumn(schema, $(outputCol), inputType, schema($(inputCol)).nullable)
    val field = new StructField("features", new VectorUDT(), true)
    StructType(schema.fields :+ field)
  }

  override val uid: String = "1234"
}
