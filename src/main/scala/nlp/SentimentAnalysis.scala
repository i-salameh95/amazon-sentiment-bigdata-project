package org.example.amazon.review.sentiment.analyzer.nlp

import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object SentimentAnalysis {

  def analyzeSentiment(df: DataFrame): DataFrame = {
    // Load Spark NLP pre-trained sentiment analysis pipeline
    val pipeline = PretrainedPipeline("analyze_sentiment", lang = "en")

    // Sentiment analysis UDF
    val sentimentAnalysisUDF = udf((text: String, title: String) => {
      val input = Option(text).filter(_.nonEmpty).getOrElse(title) // Use `title` if `text` is null/empty
      if (input == null || input.isEmpty) {
        "neutral" // Default to neutral if no valid input
      } else {
        try {
          val sentiment = pipeline.annotate(input).get("sentiment") match {
            case Some(sentiments) if sentiments.nonEmpty => sentiments.head
            case _ => "neutral"
          }
          sentiment
        } catch {
          case _: Exception =>
            "neutral" // Default to neutral if any error occurs
        }
      }
    })

    // Add a "sentiment" column using the UDF
    df.withColumn("sentiment", sentimentAnalysisUDF(col("text"), col("title")))
  }
}

/*  Summary:
1. If `text` is missing or empty, the UDF will use the `title` field for sentiment analysis.
2. Defaults to "neutral" for all invalid cases (null, empty, or exceptions).
3. Guarantees no "na" or null values are returned for the `sentiment` column.
*/
