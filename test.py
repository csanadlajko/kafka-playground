from produce.producer import produce_message
from consume.consumer import KafkaConsumer

consumer: KafkaConsumer = KafkaConsumer(
    topic_name="test-topic"
)

KAFKA_TOPIC: str = "test-topic"
PROCUCE_MESSAGE_STRING: str = "a message to consume"

produce_message(KAFKA_TOPIC, PROCUCE_MESSAGE_STRING)

latest_metadata = consumer.read_latest_message()

## build docker for kafka broker
## sudo docker compose up
