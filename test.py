from produce.producer import produce_message
from consume.consumer import read_topic

KAFKA_TOPIC: str = "test-topic"
PROCUCE_MESSAGE_STRING: str = "a message to consume"

produce_message(KAFKA_TOPIC, PROCUCE_MESSAGE_STRING)

metadata = read_topic(topic_name=KAFKA_TOPIC)

print(metadata)