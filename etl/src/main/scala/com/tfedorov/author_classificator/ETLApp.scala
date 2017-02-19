package com.tfedorov.author_classificator

import java.io.File

import FileUtils._

import scala.util.{Failure, Try}

/**
  * Created by Taras_Fedorov on 2/3/2017.
  */
object ETLApp extends App {

  case class DataPaths(inputFolder: String, outputFolder: String)

  val projectDir = "D:/work/workspace/pet_projects/authorClassificator"

  val trainPaths = DataPaths(
    s"$projectDir/output/raw/trainRawData",
    s"$projectDir/output/train")
  val testPaths = DataPaths(
    s"$projectDir/output/testRawData",
    s"$projectDir/output/test")

  val appPaths = trainPaths

  val directory = new File(appPaths.inputFolder)

  if (!directory.isDirectory) {
    println(s" Parameter - [${appPaths.inputFolder}] is not a folder")
    System.exit(0)
  }

  createIfNotExist(appPaths.outputFolder)
  println(s"Start to proccess [${directory.getAbsolutePath}]")

  for (file <- directory.listFiles) {
    val newFileName = s"${appPaths.outputFolder}/${file.getName}"

    val extract: (String) => String = readFile
    val transform: (String) => String = TokenTransformer(_)
    val load: (String) => Unit = write2NewFile(newFileName)

    val etlResult =
      Try(extract(file.getAbsolutePath))
        .map(transform(_))
        .map(load)
    if (etlResult.isFailure)
      println(etlResult.failed)
  }
  println(s"Finish successfully. See [${appPaths.outputFolder}] for result")

}
