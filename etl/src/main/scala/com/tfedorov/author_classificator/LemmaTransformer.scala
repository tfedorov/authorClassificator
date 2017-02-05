package com.tfedorov.author_classificator

import org.languagetool.language.Ukrainian
import org.languagetool.tokenizers.uk.UkrainianWordTokenizer
import org.languagetool.{AnalyzedToken, AnalyzedTokenReadings, JLanguageTool}

/**
  * Created by Taras_Fedorov on 2/3/2017.
  */
object LemmaTransformer {
  val tokenizer: UkrainianWordTokenizer = new UkrainianWordTokenizer()

  def apply(text: String): String = {

    val langTool: JLanguageTool = new JLanguageTool(new Ukrainian())
    val analyzedSentence = langTool.getAnalyzedSentence(text)
    val tokens: Array[AnalyzedTokenReadings] = analyzedSentence.getTokensWithoutWhitespace

    tokens.toList.map(_.getAnalyzedToken(0)).map(lemmaOrToken(_)).mkString(System.lineSeparator)
  }

  private def lemmaOrToken(analyzedToken: AnalyzedToken): String = {
    if (analyzedToken.getLemma == null)
      return analyzedToken.getToken
    analyzedToken.getLemma
  }


}
