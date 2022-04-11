from typing import Awaitable, Optional
import json
import logging
import requests

from aiohttp import web
from kafka import KafkaConsumer, KafkaProducer

from decapode.config import KAFKA_URI, KAFKA_API_VERSION, DECAPODE_API_URL, DATAGOUV_API_URL
from decapode.crawl import crawl_urls

log = logging.getLogger("decapode-kafka")

def produce(producer: KafkaProducer, id: str, data: dict) -> None:
    key = id.encode("utf-8")

    value = {
        'service': 'udata',
        'data': data,
    }
    producer.send(topic='decapode', value=value, key=key)
    producer.flush()


def get_latest_check(resource_id: str) -> Optional[int]:
    '''Returns None if unsuccessful at fetching latest check, otherwise returns latest check status code.'''
    resource_latest_check_url = f'{DECAPODE_API_URL}/api/checks/latest/?resource_id={resource_id}'
    try:
        return requests.get(resource_latest_check_url).json()['status']
    except json.JSONDecodeError:
        log.debug(f'Latest check was not fetched successfully for resource {resource_id}')
        return None


async def kafka_check_resource_avalability() -> Awaitable[None]:
    '''For each message received in Kafka topic dataset, check availability of all resources of the dataset:
        1. upon last check by Decapode
        2. at present time
    '''
    kafka_api_version = tuple([int(value) for value in KAFKA_API_VERSION.split('.')])
    consumer = KafkaConsumer(
        'dataset',
        bootstrap_servers=KAFKA_URI,
        group_id='decapode',
        api_version=kafka_api_version,
        reconnect_backoff_max_ms=100000,  # TODO: what value to set here?
    )

    producer = KafkaProducer(bootstrap_servers=KAFKA_URI, value_serializer=lambda v: json.dumps(v).encode('utf-8'), api_version=kafka_api_version)
    print('created producer')

    for message in consumer:
        print('Received message')
        message_contents = json.loads(message.value)
        dataset_id = message_contents['data']['id']

        dataset_api_url = f'{DATAGOUV_API_URL}/datasets/{dataset_id}/'
        try:
            # Get all resources for dataset
            resources = requests.get(dataset_api_url).json()['resources']
        except web.HTTPNotFound:
            log.debug('Dataset not found using API')

        for resource in resources:
            # Get latest check of resource
            latest_check_status = get_latest_check(resource_id=resource['id'])

            # Run a check for the resource
            await crawl_urls([{'url': resource['url']}])

            # Check in Decapode DB the output of that check
            new_check_status = get_latest_check(resource_id=resource['id'])

            # Send message with checks results to Kafka
            outbound_message_data = {'resource_id': resource['id'], 'dataset_id': dataset_id, 'previous_check': latest_check_status, 'new_check': new_check_status}
            log.debug('Sent to Kafka', outbound_message_data)
            print(outbound_message_data)
            produce(producer, id=resource['id'], data=outbound_message_data)
