# Amazon Sentiment Big Data Project

Real-time sentiment analysis pipeline for Amazon reviews using Kafka, Spark Structured Streaming, Spark NLP, and MongoDB.

This project was built as part of a Big Data course in a Master of AI program. It demonstrates a streaming pipeline that reads Amazon review data, publishes records to Kafka topics, processes them with Spark, applies sentiment analysis, and writes enriched results to MongoDB.

## Architecture

1. Python producer reads review records from the Hugging Face `McAuley-Lab/Amazon-Reviews-2023` dataset.
2. Producer enriches each review with a category and sends JSON messages to Kafka.
3. Spark Structured Streaming consumes category-specific Kafka topics.
4. Spark parses and transforms review records.
5. Spark NLP assigns a sentiment label.
6. Results are written to MongoDB.

## Repository Layout

```text
.
|-- build.sbt
|-- project/
|-- python-scripts/
|   |-- AmazonReviews.py
|   |-- requirements.txt
|   `-- .env.example
`-- src/main/scala/
    |-- consumer/KafkaConsumerModule.scala
    |-- nlp/SentimentAnalysis.scala
    `-- processing/DataParserTransformer.scala
```

## Prerequisites

- Java 17 recommended for Spark 3.5.x (the committed `.jvmopts` adds the `--add-opens` flags Spark needs on Java 17+ when run through sbt)
- sbt
- Python 3.9+
- Kafka broker
- MongoDB instance or MongoDB Atlas cluster

## Configuration

Do not commit `.env` files or real connection strings.

For the Python producer, copy the example file and adjust local values:

```bash
cd python-scripts
cp .env.example .env
```

Main environment variables:

```text
KAFKA_BROKER=localhost:9092
KAFKA_TOPICS=reviews_Digital_Music,reviews_Handmade_Products,reviews_Gift_Cards,reviews_Appliances
HUGGINGFACE_DATASET=McAuley-Lab/Amazon-Reviews-2023
ASIN_CATEGORY_FILE=data/asin2category.json
MAX_REVIEWS_PER_CATEGORY=0
MONGODB_URI=mongodb://localhost:27017
MONGODB_DATABASE=amazon_reviews
MONGODB_COLLECTION=amazon_reviews
SPARK_MASTER=local[*]
SPARK_CHECKPOINT_LOCATION=checkpoint/amazon-reviews
```

The producer can run without `ASIN_CATEGORY_FILE`; missing ASIN mappings default to `Unknown`.

`MAX_REVIEWS_PER_CATEGORY=0` sends every review in the dataset (millions of messages). Set a positive number, for example `1000`, for a quick local test.

## Run Producer

```bash
cd python-scripts
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python AmazonReviews.py
```

On Windows PowerShell, activate the environment with:

```powershell
.\venv\Scripts\Activate.ps1
```

## Run Consumer

From the repository root:

```bash
export MONGODB_URI="mongodb://localhost:27017"
sbt "runMain org.example.amazon.review.sentiment.analyzer.consumer.KafkaConsumerModule"
```

The Scala consumer reads environment variables from the shell. It does not automatically load `python-scripts/.env`.

The consumer subscribes to:

```text
reviews_Digital_Music
reviews_Handmade_Products
reviews_Gift_Cards
reviews_Appliances
```

Override this list with comma-separated `KAFKA_TOPICS`.

## Notes

- The producer uses `trust_remote_code=True` when loading the Hugging Face dataset. Use only trusted datasets or remove that flag if it is not required.
- Spark NLP may download pretrained models on first run.
- This repository does not include large datasets, local IDE settings, credentials, or generated checkpoints.
