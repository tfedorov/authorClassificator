package com.tfedorov.author_classificator

import java.io.File
import FileUtils._
/**
  * Created by Taras_Fedorov on 2/3/2017.
  */
object TransformApp extends App {


  val saveWithPrefix = (name: String, fileInDir: File) => {
    println(name)


  }
  processDirectory("etl/src/main/resources/1")(write2NewFile)

}
