import json
from kafka import KafkaConsumer
import logging

from decapode import context
from decapode.config import KAFKA_URI, KAFKA_API_VERSION

log = logging.getLogger("decapode-kafka")


async def consume_kafka_datasets() -> None:
    '''For each message received in Kafka topic dataset, set all resources of the dataset to
    priority for crawling.
    '''
    kafka_api_version = tuple([int(value) for value in KAFKA_API_VERSION.split('.')])
    consumer = KafkaConsumer(
        'resource.created',
        'resource.modified',
        'resource.deleted',
        bootstrap_servers=KAFKA_URI,
        group_id='decapode',
        api_version=kafka_api_version,
        reconnect_backoff_max_ms=100000,  # TODO: what value to set here?
        # auto_offset_reset='smallest' # This line can be added for tests to receive messages from the beginning
    )

    log.info('Consuming resource.created & resource.modified topics...')
    for message in consumer:
        log.info('Received message')
        message_contents = json.loads(message.value)
        topic = message.topic
        dataset_id = message_contents['meta']['dataset_id']
        resource = message_contents['value']['resource']

        pool = await context.pool()
        async with pool.acquire() as connection:
            if topic == 'resource.created':
                # Insert new resource in catalog table and mark as high priority for crawling
                q = f'''
                        INSERT INTO catalog (dataset_id, resource_id, url, deleted, priority, initialization)
                        VALUES ('{dataset_id}', '{resource["id"]}', '{resource["url"]}', FALSE, TRUE, FALSE)
                        ON CONFLICT (dataset_id, resource_id, url) DO UPDATE SET priority = TRUE;'''
                await connection.execute(q)
            elif topic == 'resource.deleted':
                # Mark resource as deleted in catalog table
                q = f'''UPDATE catalog SET deleted = TRUE WHERE resource_id = '{resource["id"]}';'''
                await connection.execute(q)
            elif topic == 'resource.modified':
                # Make resource high priority for crawling
                q = f'''UPDATE catalog SET priority = TRUE WHERE resource_id = '{resource["id"]}';'''
                await connection.execute(q)
            else:
                log.error(f'Unknown topic {topic}')

