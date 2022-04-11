from typing import Awaitable
import json
from kafka import KafkaConsumer
import logging
import requests

from decapode import context

from decapode.config import KAFKA_URI, KAFKA_API_VERSION, DATAGOUV_API_URL

log = logging.getLogger("decapode-kafka")


async def consume_kafka_datasets() -> None:
    '''For each message received in Kafka topic dataset, set all resources of the dataset to
    priority for crawling.
    '''
    kafka_api_version = tuple([int(value) for value in KAFKA_API_VERSION.split('.')])
    consumer = KafkaConsumer(
        'dataset',
        bootstrap_servers=KAFKA_URI,
        group_id='decapode',
        api_version=kafka_api_version,
        reconnect_backoff_max_ms=100000,  # TODO: what value to set here?
    )

    for message in consumer:
        print('Received message')
        message_contents = json.loads(message.value)
        dataset_id = message_contents['data']['id']

        dataset_api_url = f'{DATAGOUV_API_URL}/datasets/{dataset_id}/'
        # Get all resources for dataset
        resources_response = requests.get(dataset_api_url)
        if resources_response.status_code != 200:
            log.debug(f'Unable to fetch resources for dataset {dataset_id}')
            continue
        resources = resources_response.json()['resources']
        print(resources)

        resource_ids = ','.join([f"'{resource['id']}'" for resource in resources])

        pool = await context.pool()
        async with pool.acquire() as connection:
            q = f'''UPDATE catalog SET priority = TRUE WHERE resource_id IN ({resource_ids})'''
            await connection.execute(q)
