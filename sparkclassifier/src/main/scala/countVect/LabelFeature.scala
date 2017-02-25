package countVect

import org.apache.spark.ml.linalg.{SparseVector, Vectors}

/**
  * Created by Taras_Fedorov on 2/25/2017.
  */
case class LabelFeature(label: Float, features: SparseVector) {
  def *(num: Double): LabelFeature = {
    LabelFeature(label, vectorMultiple(features, num))
  }

  private def vectorMultiple(vector: SparseVector, multiPlicator: Double) = {
    Vectors.dense(vector.values.map(_ * multiPlicator)).toSparse
  }
}

