import logging

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from configs.settings import settings


logger = logging.getLogger(__name__)


def _topic_definitions() -> list[NewTopic]:
    retention_ms = str(settings.KAFKA_RETENTION_HOURS * 60 * 60 * 1000)
    topic_config = {
        "cleanup.policy": "delete",
        "retention.ms": retention_ms,
    }

    return [
        NewTopic(
            topic=settings.KAFKA_AGG_TRADES_TOPIC,
            num_partitions=settings.KAFKA_NUM_PARTITIONS,
            replication_factor=settings.KAFKA_REPLICATION_FACTOR,
            config=topic_config,
        ),
        NewTopic(
            topic=settings.KAFKA_DEAD_LETTER_TOPIC,
            num_partitions=settings.KAFKA_NUM_PARTITIONS,
            replication_factor=settings.KAFKA_REPLICATION_FACTOR,
            config=topic_config,
        ),
    ]


def create_topics() -> None:
    admin_client = AdminClient(
        {"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS}
    )
    metadata = admin_client.list_topics(timeout=10)
    existing_topics = set(metadata.topics)
    missing_topics = [
        topic
        for topic in _topic_definitions()
        if topic.topic not in existing_topics
    ]

    if not missing_topics:
        logger.info("All Kafka topics already exist.")
        return

    results = admin_client.create_topics(missing_topics, request_timeout=15)
    failures: list[str] = []

    for topic_name, result in results.items():
        try:
            result.result()
            logger.info("Created Kafka topic: %s", topic_name)
        except KafkaException as error:
            kafka_error = error.args[0]
            if kafka_error.code() == KafkaError.TOPIC_ALREADY_EXISTS:
                logger.info("Kafka topic already exists: %s", topic_name)
                continue

            failures.append(f"{topic_name}: {kafka_error}")
            logger.error("Failed to create Kafka topic %s: %s", topic_name, kafka_error)

    if failures:
        raise RuntimeError("Could not create Kafka topics: " + "; ".join(failures))


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    create_topics()


if __name__ == "__main__":
    main()
