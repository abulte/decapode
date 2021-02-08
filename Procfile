web: gunicorn app:app_factory --worker-class aiohttp.GunicornWebWorker
scheduler: python crawl.py
release: python cli.py init_db --index
