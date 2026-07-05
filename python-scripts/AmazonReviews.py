import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import islice
from pathlib import Path

from datasets import load_dataset
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv(Path(__file__).resolve().parent / ".env")


class AmazonReviews:
    def __init__(self):
        self.script_dir = Path(__file__).resolve().parent
        self.dataset_name = os.getenv("HUGGINGFACE_DATASET", "McAuley-Lab/Amazon-Reviews-2023")
        self.kafka_broker = os.getenv("KAFKA_BROKER", "localhost:9092")
        configured_category_map = Path(os.getenv("ASIN_CATEGORY_FILE", "data/asin2category.json"))
        self.category_map_path = (
            configured_category_map
            if configured_category_map.is_absolute()
            else self.script_dir / configured_category_map
        )
        self.max_reviews = int(os.getenv("MAX_REVIEWS_PER_CATEGORY", "0"))  # 0 means no limit
        self.producer = self.create_kafka_producer(self.kafka_broker)
        self.asin_to_category = self.load_asin_to_category()

    @staticmethod
    def create_kafka_producer(kafka_broker):
        """Initialize Kafka producer with JSON serialization."""
        return KafkaProducer(
            bootstrap_servers=kafka_broker,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    @staticmethod
    def read_json_file(file_path):
        """Helper to read JSON files."""
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def load_asin_to_category(self):
        """Load asin-to-category mapping."""
        if not self.category_map_path.exists():
            return {}
        return self.read_json_file(self.category_map_path)

    def prepare_enriched_review(self, review):
        """Prepare enriched review with metadata."""
        asin = review.get("asin")
        return {
            "rating": review.get("rating"),
            "title": review.get("title"),
            "text": review.get("text", "No Review Text"),
            "asin": asin,
            "timestamp": review.get("timestamp"),
            "user_id": review.get("user_id"),
            "category": self.asin_to_category.get(asin, "Unknown"),
        }

    def process_reviews(self, reviews, category):
        """Send reviews for a specific category to Kafka.

        KafkaProducer.send is asynchronous and thread-safe, so a plain loop is
        enough; batching and I/O happen on the producer's background thread.
        """
        topic = f"reviews_{category.replace(' ', '_')}"
        if self.max_reviews > 0:
            reviews = islice(reviews, self.max_reviews)
        for review in reviews:
            self.producer.send(topic, value=self.prepare_enriched_review(review)) \
                .add_errback(lambda exc: print(f"Failed to send message to {topic}: {exc}"))
        self.producer.flush()

    @staticmethod
    def handle_futures(futures):
        """Handle ThreadPool futures to manage exceptions."""
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                print(f"Failed to process future: {exc}")

    def pull_data(self, category):
        """Fetch reviews data from Hugging Face dataset."""
        dataset = load_dataset(self.dataset_name, f"raw_review_{category}", trust_remote_code=True)
        return dataset["full"]

    def process_category(self, category):
        """Process a single category."""
        reviews = self.pull_data(category)
        self.process_reviews(reviews, category)

    def run(self):
        """Main processing loop for multiple categories."""
        categories = ["Digital_Music", "Handmade_Products", "Gift_Cards", "Appliances"]
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(self.process_category, category) for category in categories]
            self.handle_futures(futures)


if __name__ == "__main__":
    AmazonReviews().run()
