package com.tfedorov.author_classificator

import org.languagetool.language.Ukrainian
import org.languagetool.rules.RuleMatch
import org.languagetool.tagging.uk.UkrainianTagger
import org.languagetool.tokenizers.uk.UkrainianWordTokenizer
import org.languagetool.{AnalyzedSentence, AnalyzedTokenReadings, JLanguageTool}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
  * Created by Taras_Fedorov on 1/26/2017.
  */
object NormalizerApp extends App {

  val PUNCTUATION_REG = "[^а-яіїє' ]".r
  val DOUBLE_SPACE_REGEX = " {2,}".r


  transform("1/1.txt", "normalized/1/1.txt");
  transform("1/2.txt", "normalized/1/2.txt");
  transform("1/3.txt", "normalized/1/3.txt");

  def writeResource(outputPath: String, normalizedTxt: String) = ???

  def readResource(inputPath: String) = ???

  private def transform(inputPath: String, outputPath: String): Unit = {

    val tryTxt = Try(readResource(inputPath))

    val tryNormalized = normailzeText(tryTxt)

    tryNormalized match {
      case Success(normalizedTxt) =>
        writeResource(outputPath, normalizedTxt)

      case Failure(e) =>
        e.printStackTrace()
    }
  }

  private def normailzeText(sentence: Try[String]) = {
    val normalized: Try[String] = sentence.map(_.toLowerCase)
      .map(PUNCTUATION_REG.replaceAllIn(_, " "))
      .map(DOUBLE_SPACE_REGEX.replaceAllIn(_, " "))

    normalized
    //println(normalized.getOrElse("Error"))
    //println(s"Hello ${sentence.map(checkSentence(_))}")
  }


  private def checkSentence(sentence: String): Unit = {
    val langTool: JLanguageTool = new JLanguageTool(new Ukrainian())
    //langTool.activateLanguageModelRules(new File("/data/google-ngram-data"));
    val analyzedSentence: AnalyzedSentence =
      langTool.getAnalyzedSentence(sentence)
    val tokens: Array[AnalyzedTokenReadings] =
      analyzedSentence.getTokensWithoutWhitespace
    val matches: List[RuleMatch] = langTool.check(sentence).asScala.toList
    val token: AnalyzedTokenReadings = tokens(3)
    token.hasLemma("новий")

    val tagger: UkrainianTagger = new UkrainianTagger()
    //tagger.getAnalyze
    val tokenizer: UkrainianWordTokenizer = new UkrainianWordTokenizer()
    val result: List[String] = tokenizer.tokenize(sentence).asScala.toList
    println("token  = [" + token.getAnalyzedToken(0).getLemma + "]")
  }
}
