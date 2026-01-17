from confluent_kafka import Consumer
from confluent_kafka.cimpl import Message
from confluent_kafka import TopicPartition
from confluent_kafka.admin import _metadata
from constants.CommonContants import KafkaCommonConstants
from typing import Any

class KafkaConsumer:

    def __init__(self, topic_name: str):
        self.topic_name = topic_name
        self.consumer = Consumer(KafkaCommonConstants.CONSUMER_DEFAULT_CONFIG)

    def read_topic(self, timeout: int = 15) -> dict[str, Any] | None:
        self.consumer.subscribe(topics=[self.topic_name])
        for _ in range(timeout):
            msg: Message = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Failed to read message from topic: {self.topic_name}\nError: {msg.error()}")
                return None
            return {
                "timestamp": msg.timestamp(),
                "value": msg.value().decode(),
                "topic_name": msg.topic(),
                "headers": msg.headers(),
                "offset": msg.offset(),
                "partition": msg.partition()
            }

    def read_latest_message(self) -> dict[str, Any]:
        topic_list: _metadata.ClusterMetadata = self.consumer.list_topics(self.topic_name, timeout=10)
        partitions: dict[int, _metadata.PartitionMetadata] = topic_list.topics[self.topic_name].partitions
        topic_partitions: list[TopicPartition] = [TopicPartition(self.topic_name, p) for p in partitions]
        for p in topic_partitions:
            low, high = self.consumer.get_watermark_offsets(p)
            p.offset = high-1
        self.consumer.assign(topic_partitions)
        msg: Message = self.consumer.poll(1.0)
        return {
            "timestamp": msg.timestamp()[1], ## epoch on index 1
            "value": msg.value().decode(),
            "topic_name": msg.topic(),
            "headers": msg.headers(),
            "offset": msg.offset(),
            "partition": msg.partition()
        }