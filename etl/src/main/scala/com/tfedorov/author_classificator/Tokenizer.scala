package com.tfedorov.author_classificator

import org.languagetool.tokenizers.uk.UkrainianWordTokenizer

import scala.collection.JavaConverters._

/**
  * Created by Taras_Fedorov on 2/3/2017.
  */
object Tokenizer {
  val tokenizer: UkrainianWordTokenizer = new UkrainianWordTokenizer()

  def apply(text: String): Seq[String] = tokenizer.tokenize(text).asScala

}
