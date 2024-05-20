package com.scalaudf

import org.apache.spark.sql.api.java.UDF1

class TermFrequencyUDF extends UDF1[Seq[String], Seq[(String, Double)]] {
  override def call(wordsList: Seq[String]): Seq[(String, Double)] = {
    val wordsCount = wordsList.size.toDouble
    val tfMap = wordsList.groupBy(identity).mapValues(_.size / wordsCount)
    tfMap.toSeq
  }
}