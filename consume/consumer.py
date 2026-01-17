from confluent_kafka import Consumer
from confluent_kafka.cimpl import Message
from constants.CommonContants import KafkaCommonConstants
from typing import Any

consumer: Consumer = Consumer(KafkaCommonConstants.CONSUMER_DEFAULT_CONFIG)

def read_topic(topic_name: str, timeout: int = 15) -> dict[str, Any] | None:
    consumer.subscribe(topics=[topic_name])
    for _ in range(timeout):
        msg: Message = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Failed to read message from topic: {topic_name}\nError: {msg.error()}")
            return None
        return {
            "timestamp": msg.timestamp(),
            "value": msg.value().decode(),
            "topic_name": msg.topic(),
            "headers": msg.headers(),
            "offset": msg.offset(),
            "partition": msg.partition()
        }