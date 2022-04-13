import asyncio
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
        group_id=None,
        api_version=kafka_api_version,
        reconnect_backoff_max_ms=100000,  # TODO: what value to set here?
        auto_offset_reset='smallest' # TODO: Check if it should be removed in prod
    )

    log.info('Consuming dataset topic...')
    for message in consumer:
        log.debug('Received message')
        message_contents = json.loads(message.value)
        dataset_id = message_contents['data']['id']

        dataset_api_url = f'{DATAGOUV_API_URL}/datasets/{dataset_id}/'
        # Get all resources for dataset
        resources_response = requests.get(dataset_api_url)
        if resources_response.status_code != 200:
            log.debug(f'Unable to fetch resources for dataset {dataset_id}')
        else:
            resources = resources_response.json()['resources']
            resource_ids = [resource['id'] for resource in resources]
            resource_id_to_url = {resource['id']: resource['url'] for resource in resources}

            pool = await context.pool()
            async with pool.acquire() as connection:
                q = f'''SELECT resource_id FROM catalog WHERE dataset_id = '{dataset_id}';'''
                resource_ids_from_catalog = await connection.fetch(q)
                resource_ids_from_catalog = [str(r['resource_id']) for r in resource_ids_from_catalog]
                resource_ids_to_add = [r for r in resource_ids if r not in resource_ids_from_catalog]
                
                for resource_to_add in resource_ids_to_add:
                    # Insert new resources in catalog table
                    q = f'''
                            INSERT INTO catalog (dataset_id, resource_id, url, deleted, priority, initialization) 
                            VALUES ('{dataset_id}', '{resource_to_add}', '{resource_id_to_url[resource_to_add]}', FALSE, TRUE, FALSE);'''
                    await connection.execute(q)

                resource_ids_to_remove = [f"'{r}'" for r in resource_ids_from_catalog if r not in resource_ids]
                resource_ids_to_remove_str = ','.join(resource_ids_to_remove)
                if len(resource_ids_to_remove_str) > 0:
                    # Mark as deleted resources that are no longer present in dataset
                    q = f'''UPDATE catalog SET deleted = TRUE WHERE resource_id IN ({resource_ids_to_remove_str});'''
                    await connection.execute(q)

                resource_ids_str = ','.join([f"'{resource['id']}'" for resource in resources])
                if len(resource_ids_str) > 0:
                    # Make resources of dataset high priority for crawling
                    q = f'''UPDATE catalog SET priority = TRUE WHERE resource_id IN ({resource_ids_str});'''
                    await connection.execute(q)
