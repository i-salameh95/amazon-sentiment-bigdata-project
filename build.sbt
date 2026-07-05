ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.20"

lazy val root = (project in file("."))
  .settings(
    name := "AmazonReviewSentimentAnalyzer",
    idePackagePrefix := Some("org.example.amazon.review.sentiment.analyzer"),
    libraryDependencies ++= Seq(
      // Spark
      "org.apache.spark" %% "spark-core" % "3.5.3",
      "org.apache.spark" %% "spark-sql" % "3.5.3",
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.3",
      // Required by Spark NLP
      "org.apache.spark" %% "spark-mllib" % "3.5.3",

      // MongoDB sink
      "org.mongodb.spark" %% "mongo-spark-connector" % "10.4.0",

      // NLP
      "com.johnsnowlabs.nlp" %% "spark-nlp" % "5.4.1"
    )
  )
