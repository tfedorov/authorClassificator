package org.apache.spark.ml.linalg

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util.{DefaultParamsWritable, SchemaUtils}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Created by Taras_Fedorov on 4/14/2017.
  */
object ListWordsCount {

  val LIST_WORDS_DEFAULT = Seq("від", "навіть", "про", "які", "до", "та", "як", "із", "що", "під", "на", "не", "для", "за", "тому", "це")
}

class ListWordsCount(stopWords: Seq[String] = ListWordsFreq.LIST_WORDS_DEFAULT) extends Transformer with HasInputCol with HasOutputCol with DefaultParamsWritable {

  val LIST_WORDS_LABEL_TERMS = Seq(LabelTextCount(1.0.toFloat, stopWords, stopWords.size))

  val sparkSession = SparkSession.builder().getOrCreate()

  import sparkSession.implicits._

  val cvModel: CountVectorizerModel = new CountVectorizer()
    .setInputCol("allText")
    .setOutputCol("features")
    .fit(LIST_WORDS_LABEL_TERMS.toDS())


  override def transform(dataset: Dataset[_]): DataFrame = {

    val countVectorizeDF = cvModel.transform(dataset)
    countVectorizeDF
    /*
        val wordFreqUDF = udf { document: Seq[String] =>
          1.0 / document.size
        }
        val freqWordDF = countVectorizeDF.withColumn("oneWordFreq", wordFreqUDF(col("allText")))
        val assembler = new VectorAssembler().
          setInputCols(Array("countedFeatures")).
          setOutputCol("assembledWordFreq")

        val assembledWordFreqDF = assembler.transform(freqWordDF)

        val interaction = new Interaction()
          .setInputCols(Array("oneWordFreq", "assembledWordFreq"))
          .setOutputCol("features")

        interaction.transform(assembledWordFreqDF)
        */
  }

  override def copy(extra: ParamMap): Transformer

  = ???

  override def transformSchema(schema: StructType): StructType

  = {
    SchemaUtils.appendColumn(schema, "features", new VectorUDT(), false)
  }

  override val uid: String =
    "1234"

}