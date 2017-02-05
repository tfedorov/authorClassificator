package com.tfedorov.author_classificator

import java.io.File

import FileUtils._

import scala.util.{Failure, Try}

/**
  * Created by Taras_Fedorov on 2/3/2017.
  */
object ETLApp extends App {

  private val INPUT_FOLDER = "etl/src/main/resources/inputRawData"
  private val OUTPUT_FOLDER = "D:\\work\\workspace\\pet_projects\\authorClassificator\\output"

  val directory = new File(INPUT_FOLDER)

  if (!directory.isDirectory) {
    println(s" Parameter - [${INPUT_FOLDER}] is not a folder")
    System.exit(0)
  }

  createIfNotExist(OUTPUT_FOLDER)
  println(s"Start to proccess [${directory.getAbsolutePath}]")

  for (file <- directory.listFiles) {
    val newFileName = s"${OUTPUT_FOLDER}/${file.getName}"

    val extract: (String) => String = readFile
    val transform: (String) => String = LemmaTransformer(_)
    val load: (String) => Unit = write2NewFile(newFileName)

    val etlResult = Try(extract(file.getAbsolutePath))
      .map(transform(_))
      .map(load)
    if (etlResult.isFailure)
      println(etlResult.failed)
  }
  println(s"Finish successfully. See [$OUTPUT_FOLDER] for result")

}
