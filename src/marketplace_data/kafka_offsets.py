"""Capture exclusive Kafka high offsets without joining or committing a group."""

import json
import sys

from confluent_kafka import Consumer, TopicPartition

from marketplace_data.simulator import TOPICS


def main() -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": sys.argv[1],
            "group.id": "stress-offset-inspection",
            "enable.auto.commit": False,
        }
    )
    try:
        metadata = consumer.list_topics(timeout=15)
        offsets = {}
        for topic in TOPICS.values():
            partitions = metadata.topics[topic].partitions
            offsets[topic] = {
                str(partition): consumer.get_watermark_offsets(
                    TopicPartition(topic, partition), timeout=15, cached=False
                )[1]
                for partition in partitions
            }
        sys.stdout.write(json.dumps(offsets) + "\n")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
