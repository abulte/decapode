web: gunicorn app:app --worker-class aiohttp.GunicornWebWorker
scheduler: python crawl.py
release: python cli.py init_db --index
