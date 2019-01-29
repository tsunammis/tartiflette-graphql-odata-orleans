import os
from aiohttp import web

from tartiflette import Engine
from tartiflette_aiohttp import register_graphql_handlers

from open_data_orleans.aiohttp_utils import on_cleanup, on_startup 
import open_data_orleans.resolvers


engine = Engine(
    os.path.dirname(os.path.abspath(__file__)) + "/sdl/default.gql"
)


def run():
    app = web.Application()

    app["config"] = {
        "redis": {
            "host": "redis://localhost:6379"
        }
    }

    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)

    ctx = {}

    web.run_app(
        register_graphql_handlers(
            app=app,
            engine=engine,
            executor_context=ctx,
            executor_http_endpoint='/graphql',
            executor_http_methods=['POST', 'GET']
        )
    )