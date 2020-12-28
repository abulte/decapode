import json
import os

import asyncpg

from aiohttp import web
from marshmallow import Schema, fields

DATABASE_URL = os.getenv('DATABASE_URL', 'postgres://postgres:postgres@localhost:5432/postgres')

routes = web.RouteTableDef()


class CheckSchema(Schema):
    # FIXME: not a check id, but a catalog one
    id = fields.Integer()
    url = fields.Str()
    domain = fields.Str()
    created_at = fields.DateTime()
    status = fields.Integer()
    headers = fields.Function(lambda obj: json.loads(obj["headers"]))
    timeout = fields.Boolean()
    response_time = fields.Float()
    error = fields.Str()
    dataset_id = fields.Str()
    resource_id = fields.UUID()
    deleted = fields.Boolean()


@routes.get("/checks/latest/")
async def get_check(request):
    url = request.query.get("url")
    resource_id = request.query.get("resource_id")
    if not url and not resource_id:
        raise web.HTTPBadRequest()
    column = "url" if url else "resource_id"
    q = f"""
    SELECT * from checks, catalog
    WHERE checks.id = catalog.last_check
    AND catalog.{column} = $1
    """
    data = await request.app["pool"].fetchrow(q, url or resource_id)
    if not data:
        raise web.HTTPNotFound()
    if data["deleted"]:
        raise web.HTTPGone()
    return web.json_response(CheckSchema().dump(dict(data)))


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
