import aioredis

async def on_startup(app):
    app["redis"] = await aioredis.create_redis_pool(app["config"]["redis"]["host"])


async def on_cleanup(app):
    app["redis"].close()
    await app["redis"].wait_closed()
