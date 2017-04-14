package com.tfedorov.aturho.spark.countVect

import com.tfedorov.aturho.spark.{AbstractSparkTest, model}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.{ListWordsCount, SparseVector}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, input_file_name}
import org.testng.annotations.Test

case class SentenceLabel(sentence: String, label: Float) {
  def +(newOne: model.SentenceLabel): model.SentenceLabel = {
    model.SentenceLabel(this.sentence + " " + newOne.sentence, this.label)
  }
}

class ListWordsCountTest extends AbstractSparkTest with Serializable {

  @Test
  def testRegexTokenizer(): Unit = {
    val trainRDD = sparkSession.read.text("../output/raw/trainRawData*").select(input_file_name, col("value"))
      .rdd.map(file => SentenceLabel(textFromFile(file), labelFromFile(file, 83)))

    val testRDD = sparkSession.read.text("../output/raw/testRawData*").select(input_file_name, col("value"))
      .rdd.map(file => SentenceLabel(textFromFile(file), labelFromFile(file, 82)))

    import sparkSession.implicits._
    val trainDS = trainRDD.toDS()
    val testDS = testRDD.toDS()

    val regexTokenizer = new RegexTokenizer()
      .setInputCol("sentence")
      .setOutputCol("allText")
      .setPattern("""[ ,.!?№()-/—\\"_$]""") // alternatively .setPattern("\\w+").setGaps(false)

    val listWordsCount = new ListWordsCount()

    val mlr = new LogisticRegression()
      .setMaxIter(1000)
      .setRegParam(0.1)
      .setFeaturesCol("features")
      .setElasticNetParam(0.1)
      .setFamily("multinomial")

    val sqlTrans = new SQLTransformer().setStatement(
      "SELECT *, (label == prediction) AS Diff FROM __THIS__")

    val pipeline = new Pipeline().setStages(Array(regexTokenizer, listWordsCount, mlr, sqlTrans))

    val model = pipeline.fit(trainDS)

    val trainRes = model.transform(trainDS)
    val testRes = model.transform(testDS)

    println("***************TRAIN RESULTS******")
    trainRes.show
    trainRes.select("features").foreach(r => println(r.get(0).asInstanceOf[SparseVector].values.mkString(",")))
    println("***********************************\n")
    println("***************TEST RESULTS********")
    testRes.select("features").foreach(r => println(r.get(0).asInstanceOf[SparseVector].values.mkString(",")))
    testRes.show
    println("***********************************")

  }

  private def textFromFile(file: Row) = {
    file.getString(1).toLowerCase
  }

  private def labelFromFile(file: Row, labelPosition: Int) = {
    (file.get(0).toString.charAt(labelPosition).asDigit).toFloat
  }
}