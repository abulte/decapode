import json
import logging
import os

import asyncpg

from aiohttp import web
from marshmallow import Schema, fields

DATABASE_URL = os.getenv('DATABASE_URL', 'postgres://postgres:postgres@localhost:5432/postgres')

log = logging.getLogger('aiohttp.access')

routes = web.RouteTableDef()


class CheckSchema(Schema):
    check_id = fields.Integer(data_key="id")
    catalog_id = fields.Integer()
    url = fields.Str()
    domain = fields.Str()
    created_at = fields.DateTime()
    status = fields.Integer()
    headers = fields.Function(lambda obj: json.loads(obj["headers"]) if obj["headers"] else {})
    timeout = fields.Boolean()
    response_time = fields.Float()
    error = fields.Str()
    dataset_id = fields.Str()
    resource_id = fields.UUID()
    deleted = fields.Boolean()


def _get_args(request):
    url = request.query.get("url")
    resource_id = request.query.get("resource_id")
    if not url and not resource_id:
        raise web.HTTPBadRequest()
    return url, resource_id


@routes.get("/checks/latest/")
async def get_check(request):
    url, resource_id = _get_args(request)
    column = "url" if url else "resource_id"
    q = f"""
    SELECT catalog.id as catalog_id, checks.id as check_id, *
    FROM checks, catalog
    WHERE checks.id = catalog.last_check
    AND catalog.{column} = $1
    """
    data = await request.app["pool"].fetchrow(q, url or resource_id)
    if not data:
        raise web.HTTPNotFound()
    if data["deleted"]:
        raise web.HTTPGone()
    return web.json_response(CheckSchema().dump(dict(data)))


@routes.get("/checks/all/")
async def get_checks(request):
    url, resource_id = _get_args(request)
    column = "url" if url else "resource_id"
    q = f"""
    SELECT catalog.id as catalog_id, checks.id as check_id, *
    FROM checks, catalog
    WHERE catalog.{column} = $1
    AND catalog.url = checks.url
    ORDER BY created_at DESC
    """
    data = await request.app["pool"].fetch(q, url or resource_id)
    if not data:
        raise web.HTTPNotFound()
    return web.json_response([CheckSchema().dump(dict(r)) for r in data])


async def app_factory():
    async def app_startup(app):
        app["pool"] = await asyncpg.create_pool(dsn=DATABASE_URL)

    async def app_cleanup(app):
        if "pool" in app:
            await app["pool"].close()

    app = web.Application()
    app.add_routes(routes)
    app.on_startup.append(app_startup)
    app.on_cleanup.append(app_cleanup)
    return app


if __name__ == "__main__":
    web.run_app(app_factory())
