# -- crawler settings -- #

# sql LIKE syntax
EXCLUDED_PATTERNS = [
    'http%data.gouv.fr%',
    # opendatasoft shp
    '%?format=shp%',
]
# max number of _completed_ requests per domain per period
BACKOFF_NB_REQ = 180
BACKOFF_PERIOD = 360  # in seconds
# crawl batch size, beware of open file limits
BATCH_SIZE = 100
# crawl url if last check is older than
SINCE = '1w'
