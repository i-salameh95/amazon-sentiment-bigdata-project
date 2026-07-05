package org.example.amazon.review.sentiment.analyzer.processing

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object DataParserTransformer {

  def processReviews(rawStream: DataFrame): DataFrame = {

    // Define the schema for the JSON structure
    val schema = new StructType()
      .add("rating", DoubleType)
      .add("title", StringType)
      .add("text", StringType)
      .add("asin", StringType)
      .add("timestamp", LongType)
      .add("user_id", StringType)
      .add("category", StringType)


    // Parse the JSON and extract fields
    rawStream.select(from_json(col("value"), schema).alias("data"))
      .select("data.*")
      .withColumn("timestamp", to_timestamp(from_unixtime(col("timestamp") / 1000)))
  }
}
