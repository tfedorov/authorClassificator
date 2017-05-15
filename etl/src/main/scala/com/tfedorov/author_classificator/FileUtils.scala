package com.tfedorov.author_classificator

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.{Files, Paths}

import scala.io.Source

/**
  * Created by Taras_Fedorov on 1/29/2017.
  */
object FileUtils {

  private val RESOURCES_DIR = "src/main/resources/"

  def readResource(resourcePath: String): String = Source.fromFile(resourcePath).getLines.mkString

  def readFile(absolutePath: String): String = Source.fromFile(absolutePath).getLines.mkString

  //def writeResource(resourcePath: String, content: String) = File(s"$RESOURCES_DIR$resourcePath").writeAll(content)

  def write2NewFile(targetFileName: String)(fileContent: String) = {
    val file = new File(targetFileName)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(fileContent)
    bw.close()
  }

  def fileExist(path: String) = Files.exists(Paths.get(path))

  def createIfNotExist(path: String): Unit = {
    if (fileExist(path))
      return
    // println(s"Folder $path already exis")
    val newDir = new File(path);
    newDir.mkdir() match {
      case true => println(s"Folder $path successfully created")
      case false => println(s"Folder $path NOT created")
    }
  }

}
