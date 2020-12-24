# decapode 🦀

`decapode` is an async metadata crawler for [data.gouv.fr](https://www.data.gouv.fr).

URLs are crawled via _aiohttp_, catalog and crawled metadata are stored in a _PostgreSQL_ database.

![](docs/screenshot.png)

## CLI

### Create database structure

`python cli.py init-db`

### Load (UPSERT) latest catalog version from data.gouv.fr

`python cli.py load-catalog`

### Print summary

```
$ python cli.py summary
-------------  ------  ------
Left to check       0
Checked        111482  100.0%
Errors           4217  3.8%
Timeouts         1204  1.1%
Replied        106061  95.1%
-------------  ------  ------

  HTTP code    Count
-----------  -------
        200    88944
        404    10144
        500     2913
        502     2760
        429      700
        400      673
        501      655
        403      236
        405       80
        503       44
        202       39
        401       14
        530        1
        410        1
```

## Crawler

`python crawl.py`

It will crawl (forever) the catalog according to config set in `config.py`.

`BATCH_SIZE` URLs are queued at each loop run.

The crawler will start with URLs never checked and then proceed with URLs crawled before `SINCE` interval. It will then wait until something changes (catalog or time).

There's a by-domain backoff mecanism. The crawler will wait when, for a given domain in a given batch, `BACKOFF_NB_REQ` is exceeded in a period of `BACKOFF_PERIOD` seconds. It will sleep and retry until the backoff is lifted.

If an URL matches one of the `EXCLUDED_PATTERNS`, it will never be checked.
