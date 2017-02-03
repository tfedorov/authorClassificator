package com.tfedorov.author_classificator

import java.io.{File, FileWriter}

import scala.io.Source
import scala.util.Try

/**
  * Created by Taras_Fedorov on 1/29/2017.
  */
object FileUtils {


  private val RESOURCES_DIR = "src/main/resources/"

  def readResource(resourcePath: String): String = Source.fromResource(resourcePath).getLines.mkString

  //def writeResource(resourcePath: String, content: String) = File(s"$RESOURCES_DIR$resourcePath").writeAll(content)

  def write2NewFile(targetFileName: String, source: File) = {
    val fw = new FileWriter(targetFileName + "_suffix");
    fw.write(source.getName)
    fw.close()
  }

  def processDirectory(path: String)(processFile: (String, File) => Unit): Unit = {
    val directory = new File(path)
    if (directory == null || directory.listFiles == null)
      return println("Null")
    for (file <- directory.listFiles)
      yield processFile(file.getAbsolutePath, file)

  }
}
