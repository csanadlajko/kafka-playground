from abc import ABC
from typing import Any

class KafkaCommonConstants:

    CONSUMER_DEFAULT_CONFIG: dict[str, str] = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "test-group-id_new",
        "auto.offset.reset": "earliest"
    }

    PRODUCER_DEFAULT_CONFIG: dict[str, str] = {
        "bootstrap.servers": "localhost:9092"
    }
