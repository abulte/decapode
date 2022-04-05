from typing import Awaitable, Optional
import json
import logging
import os
import resource
import requests

from aiohttp import web
import asyncio
from kafka import KafkaConsumer, KafkaProducer

from decapode.crawl import crawl_urls

KAFKA_HOST = os.environ.get('KAFKA_HOST', 'localhost')
KAFKA_PORT = os.environ.get('KAFKA_PORT', '9092')
KAFKA_URI = f'{KAFKA_HOST}:{KAFKA_PORT}'
KAFKA_API_VERSION = os.environ.get('KAFKA_API_VERSION', '2.5.0')

DECAPODE_API_URL = 'http://dev.local:8000'

# DATAGOUV_API_URL = 'https://www.data.gouv.fr/api/1'
DATAGOUV_API_URL = 'http://dev.local:7000/api/1'

log = logging.getLogger("decapode-kafka")


consumer = KafkaConsumer(
    'dataset',
    bootstrap_servers=KAFKA_URI,
    group_id='decapode',
    reconnect_backoff_max_ms=100000,  # TODO: what value to set here?
)

producer = KafkaProducer(bootstrap_servers=KAFKA_URI, value_serializer=lambda v: json.dumps(v).encode('utf-8'))


def produce(producer: KafkaProducer, id: str, data) -> None:
    key = id.encode("utf-8")

    value = {
        'service': 'udata',
        'data': data,
    }
    producer.send(topic='decapode', value=value, key=key)
    producer.flush()


async def run_new_check(resource_url: str) -> Awaitable[None]:
    '''Runs crawler only on the given resource URL'''
    urls = await crawl_urls([{'url': resource_url}])


def get_latest_check(resource_id: str) -> Optional[int]:
    '''Returns None if unsuccessful at fetching latest check, otherwise returns latest check status code.'''
    resource_latest_check_url = f'{DECAPODE_API_URL}/api/checks/latest/?resource_id={resource["id"]}'
    try:
        return requests.get(resource_latest_check_url).json()['status']
    except json.JSONDecodeError:
        log.debug(f'Latest check was not fetched successfully for resource {resource_id}')
        return None


for message in consumer:
    message_contents = json.loads(message.value)
    dataset_id = message_contents['data']['id']
    organization_id = message_contents['data']['organization']['id']

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
        asyncio.run(run_new_check(resource['url']))

        # Check in Decapode DB the output of that check
        new_check_status = get_latest_check(resource_id=resource['id'])

        # Send message with checks results to Kafka
        produce(producer, id=resource['id'], data={'resource_id': resource['id'], 'dataset_id': dataset_id, 'previous_check': latest_check_status, 'new_check': new_check_status})
