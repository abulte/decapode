import os
import logging

from unittest.mock import MagicMock

import asyncpg

from decapode.monitor import Monitor

log = logging.getLogger("decapode")
context = {}


def monitor():
    if "monitor" in context:
        return context["monitor"]
    curses_enabled = os.getenv("DECAPODE_CURSES_ENABLED", False) == "True"
    if curses_enabled:
        monitor = Monitor()
    else:
        monitor = MagicMock()
        monitor.set_status = lambda x: log.debug(x)
        monitor.init = lambda **kwargs: log.debug(f"Starting decapode... {kwargs}")
    context["monitor"] = monitor
    return context["monitor"]


async def pool():
    if "pool" not in context:
        dsn = os.getenv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/postgres")
        context["pool"] = await asyncpg.create_pool(dsn=dsn, max_size=50)
    return context["pool"]
