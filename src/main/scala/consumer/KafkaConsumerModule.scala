package org.example.amazon.review.sentiment.analyzer.consumer

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.example.amazon.review.sentiment.analyzer.nlp.SentimentAnalysis
import org.example.amazon.review.sentiment.analyzer.processing.DataParserTransformer

object KafkaConsumerModule {

  def main(args: Array[String]): Unit = {
    // Application Configuration
    val brokers = sys.env.getOrElse("KAFKA_BROKER", "localhost:9092")

    val defaultTopics = Seq(
      "reviews_Digital_Music",
      "reviews_Handmade_Products",
      "reviews_Gift_Cards",
      "reviews_Appliances"
    )
    val topics = sys.env.get("KAFKA_TOPICS")
      .map(_.split(",").map(_.trim).filter(_.nonEmpty).toSeq)
      .filter(_.nonEmpty)
      .getOrElse(defaultTopics)
    // Create Spark Session
    val spark = createSparkSession()
    // Start Structured Streaming Kafka Consumer
    startKafkaStructuredStreaming(spark, brokers, topics)
  }

  def createSparkSession(): SparkSession = {
    SparkSession.builder()
      .appName("AmazonReviewsKafkaConsumer")
      .master(sys.env.getOrElse("SPARK_MASTER", "local[*]"))
      .getOrCreate()
  }

  def startKafkaStructuredStreaming(spark: SparkSession, brokers: String, topics: Seq[String]): Unit = {
    val mongoUri = sys.env.getOrElse("MONGODB_URI", "mongodb://localhost:27017")
    val mongoDatabase = sys.env.getOrElse("MONGODB_DATABASE", "amazon_reviews")
    val mongoCollection = sys.env.getOrElse("MONGODB_COLLECTION", "amazon_reviews")
    val checkpointLocation = sys.env.getOrElse("SPARK_CHECKPOINT_LOCATION", "checkpoint/amazon-reviews")

    // Create DataFrame representing Kafka stream
    val kafkaStream: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribe", topics.mkString(","))
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .load()

    // Parse Kafka messages
    val rawStream = kafkaStream
      .selectExpr("CAST(value AS STRING)")

    // Apply Data Parsing and Transformation
    val transformedStream = DataParserTransformer.processReviews(rawStream)

    // Apply Sentiment Analysis to the transformed stream
    val sentimentStream = SentimentAnalysis.analyzeSentiment(transformedStream)


    val query = sentimentStream.writeStream
      .format("mongodb")
      .option("spark.mongodb.connection.uri", mongoUri)
      .option("spark.mongodb.database", mongoDatabase)
      .option("spark.mongodb.collection", mongoCollection)
      .option("checkpointLocation", checkpointLocation)
      .outputMode("append")
      .start()

    // Await termination
    query.awaitTermination()
  }
}
