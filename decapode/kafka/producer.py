import json
from kafka import KafkaProducer

from decapode.config import KAFKA_URI


class KafkaProducerSingleton:
    __instance = None

    @staticmethod
    def get_instance() -> KafkaProducer:
        if KafkaProducerSingleton.__instance is None:
            KafkaProducerSingleton.__instance = KafkaProducer(
                bootstrap_servers=KAFKA_URI,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
        return KafkaProducerSingleton.__instance


def produce(id, data: dict, message_type: str):
    '''
    Produce message with marshalled document.
    kwargs is meant to contain non generic values
    for the meta fields of the message.
    '''
    producer = KafkaProducerSingleton.get_instance()
    key = id.encode("utf-8")

    value = {
        'service': 'decapode',
        'data': data,
        'meta': {
            'message_type': message_type
        }
    }

    producer.send(topic='resource-crawler', value=value, key=key)
    producer.flush()
