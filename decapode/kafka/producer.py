import json

from kafka import KafkaProducer

from decapode.config import KAFKA_URI, KAFKA_API_VERSION


kafka_api_version = tuple([int(value) for value in KAFKA_API_VERSION.split('.')])
producer = KafkaProducer(bootstrap_servers=KAFKA_URI, value_serializer=lambda v: json.dumps(v).encode('utf-8'), api_version=kafka_api_version)


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


def produce(id, data: dict):
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
    }

    producer.send(topic='decapode', value=value, key=key)
    producer.flush()
