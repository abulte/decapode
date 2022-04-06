import os
# -- crawler settings -- #

# sql LIKE syntax
EXCLUDED_PATTERNS = [
    'http%geo.data.gouv.fr%',
    # opendatasoft shp
    '%?format=shp%',
]
# no backoff for those domains
NO_BACKOFF_DOMAINS = [
    'static.data.gouv.fr',
    'www.data.gouv.fr',
    # dead domain, no need to backoff
    'inspire.data.gouv.fr',
]
# domains which we use GET for by default
GET_DOMAINS = []
# max number of _completed_ requests per domain per period
BACKOFF_NB_REQ = 180
BACKOFF_PERIOD = 360  # in seconds
# crawl batch size, beware of open file limits
BATCH_SIZE = 100
# crawl url if last check is older than
SINCE = '1w'
# seconds to wait for between batches
SLEEP_BETWEEN_BATCHES = 60

# -- Kafka integration config -- #
KAFKA_HOST = os.environ.get('KAFKA_HOST', 'localhost')
KAFKA_PORT = os.environ.get('KAFKA_PORT', '9092')
KAFKA_URI = f'{KAFKA_HOST}:{KAFKA_PORT}'
KAFKA_API_VERSION = os.environ.get('KAFKA_API_VERSION', '2.5.0')

DECAPODE_API_URL = os.environ.get('DECAPODE_API_URL', 'http://dev.local:8000')
DATAGOUV_API_URL = os.environ.get('DATAGOUV_API_URL', 'http://dev.local:7000/api/1')
