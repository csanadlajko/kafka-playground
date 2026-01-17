from confluent_kafka import Producer
from confluent_kafka.cimpl import Message
from constants.CommonContants import KafkaCommonConstants
from typing import Any

class KafkaProducer:

    def __init__(self, topic_name: str, message: Any):
        self.topic_name = topic_name
        self.message = message ## currently only accepting string messages
        self.producer = Producer(KafkaCommonConstants.PRODUCER_DEFAULT_CONFIG)

    def _log_message(self, error, message: Message):
        if error is not None:
            print(f"Message delivery failed: {error}")
        else:
            print(f"Message delivered to topic: {message.topic()} [part: {message.partition()}] - [offs: {message.offset()}]")

    def produce_message(self) -> None:
        self.producer.produce(self.topic_name, self.message.encode(), callback=self._log_message)
        self.producer.flush()
