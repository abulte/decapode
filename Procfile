web: gunicorn decapode.app:app_factory --worker-class aiohttp.GunicornWebWorker
scheduler: decapode-crawl
release: decapode init_db --index
