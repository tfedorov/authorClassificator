package com.tfedorov.author_classificator

import org.languagetool.tokenizers.uk.UkrainianWordTokenizer

import scala.collection.JavaConverters._
/**
  * Created by Taras_Fedorov on 2/5/2017.
  */
object TokenTransformer {
  val tokenizer: UkrainianWordTokenizer = new UkrainianWordTokenizer()

  def apply(text: String): String = {
    tokenizer.tokenize(text).asScala.filter(_.length > 1).map(_.toLowerCase).mkString(System.lineSeparator)
  }
}
