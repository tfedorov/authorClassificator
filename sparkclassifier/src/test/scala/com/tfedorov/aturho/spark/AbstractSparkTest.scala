package com.tfedorov.aturho.spark

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.testng.Assert
import org.testng.annotations.BeforeClass

/**
  * Created by Taras_Fedorov on 2/17/2017.
  */
class AbstractSparkTest extends Assert {

  var sqlContext: SparkSession = null
  var sc: SparkContext = null

  @BeforeClass
  def initTest(): Unit = {
    sqlContext = SparkSession.builder.
      master("local")
      .appName("spark test")
      .getOrCreate()

    sc = sqlContext.sparkContext
  }


}
