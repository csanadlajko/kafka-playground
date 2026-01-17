from confluent_kafka import Producer
from confluent_kafka.cimpl import Message
from constants.CommonContants import KafkaCommonConstants

producer: Producer = Producer(KafkaCommonConstants.PRODUCER_DEFAULT_CONFIG)

def log_message(error, message: Message):
    if error is not None:
        print(f"Message delivery failed: {error}")
    else:
        print(f"Message delivered to topic: {message.topic()} [{message.partition()}] - [{message.offset()}]")

def produce_message(topic_name: str, message: str) -> None:
    producer.produce(topic_name, message.encode(), callback=log_message)
    producer.flush()