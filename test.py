from produce.producer import KafkaProducer
from consume.consumer import KafkaConsumer

KAFKA_TOPIC: str = "test-topic"
PRODUCE_MESSAGE_STRING: str = "a message to consume"

consumer: KafkaConsumer = KafkaConsumer(
    topic_name=KAFKA_TOPIC
)

producer: KafkaProducer = KafkaProducer(
    topic_name=KAFKA_TOPIC,
    message=PRODUCE_MESSAGE_STRING
)

producer.produce_message()

latest_metadata = consumer.read_latest_message()

print(latest_metadata)

## build docker for kafka broker
## sudo docker compose up
