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

## API

### Run

```
pip install -r requirements.dev.txt
adev runserver app.py
```

### Get a check by url

```
$ curl -s "http://localhost:8000/checks/?url=http://opendata-sig.saintdenis.re/datasets/661e19974bcc48849bbff7c9637c5c28_1.csv" | json_pp
{
   "created_at" : "2020-12-24T18:18:19.429045",
   "resource_id" : "b3678c59-5b35-43ad-9379-fce29e5b56fe",
   "timeout" : false,
   "headers" : {
      "x-amz-meta-contentlastmodified" : "2018-11-19T09:38:28.490Z",
      "date" : "Thu, 24 Dec 2020 18:18:19 GMT",
      "x-amz-meta-cachetime" : "191",
      "last-modified" : "Wed, 29 Apr 2020 02:19:04 GMT",
      "etag" : "\"20415964703d9ccc4815d7126aa3a6d8\"",
      "content-type" : "text/csv",
      "server" : "openresty",
      "content-encoding" : "gzip",
      "cache-control" : "must-revalidate",
      "content-length" : "207",
      "content-disposition" : "attachment; filename=\"xn--Dlimitation_des_cantons-bcc.csv\"",
      "connection" : "keep-alive",
      "vary" : "Accept-Encoding"
   },
   "domain" : "opendata-sig.saintdenis.re",
   "status" : 200,
   "id" : 64148,
   "url" : "http://opendata-sig.saintdenis.re/datasets/661e19974bcc48849bbff7c9637c5c28_1.csv",
   "response_time" : 0.705086946487427,
   "error" : null,
   "deleted" : false,
   "dataset_id" : "5c34944606e3e73d4a551889"
}
```
