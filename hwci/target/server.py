from aiohttp import web
import argparse
import asyncio
import contextvars
import logging
import pydantic
import typing

import hwci.cas
import hwci.target.ci

logger = logging.getLogger(__name__)

ENGINE = contextvars.ContextVar("hwci.target.server.ENGINE")

routes = web.RouteTableDef()


class NewRunData(pydantic.BaseModel):
    run_id: str
    device: str
    tftp: typing.Dict[str, str]
    image: typing.Optional[str]


@routes.post("/runs/{run_id}/files")
async def post_files(request):
    run_id = request.match_info["run_id"]
    buf = await request.content.read()

    engine = ENGINE.get()
    run = engine.get_run(run_id)

    deserializer = hwci.cas.Deserializer(buf)
    writes = []
    hdigests = []
    while True:
        item = deserializer.deserialize()
        if item is None:
            break
        digest, objbuf = item
        hdigest = digest.hex()
        writes.append((hdigest, objbuf))
        hdigests.append(hdigest)

    engine.cas.write_many_object_buffers(writes)
    logger.info("Received %d objects", len(writes))

    run.notify_objects(hdigests)

    return web.Response(text="OK")


@routes.post("/runs")
async def post_runs(request):
    engine = ENGINE.get()
    data = NewRunData.model_validate(await request.json())

    device = engine.get_device(data.device)
    engine.new_run(data.run_id, device, tftp=data.tftp, image=data.image)

    return web.Response(text="OK")


@routes.get("/runs/{run_id}/missing")
async def get_missing(request):
    run_id = request.match_info["run_id"]

    engine = ENGINE.get()
    run = engine.get_run(run_id)

    return web.json_response(
        [hwci.cas.make_hdigest(digest) for digest in run.missing_objects()]
    )


@routes.post("/runs/{run_id}/launch")
async def post_launch(request):
    engine = ENGINE.get()
    run_id = request.match_info["run_id"]

    run = engine.get_run(run_id)
    run.submit()

    return web.Response(text="OK")


@routes.post("/runs/{run_id}/terminate")
async def post_terminate(request):
    engine = ENGINE.get()
    run_id = request.match_info["run_id"]

    run = engine.get_run(run_id)
    run.terminate()

    return web.Response(text="OK")


@routes.post("/runs/{run_id}/updates")
async def post_updates(request):
    engine = ENGINE.get()
    run_id = request.match_info["run_id"]

    run = engine.get_run(run_id)

    response = web.StreamResponse()
    await response.prepare(request)

    async for buf in run.iter_logs():
        await response.write(buf)
    await response.write_eof()

    return response

    return web.Response(text="OK")


async def prune_periodically(engine):
    while True:
        try:
            keep_hdigests = set()
            for run in engine.get_all_runs():
                keep_hdigests.update(run.get_root_objects())
            engine.cas.prune(keep_hdigests)
        except Exception:
            logger.exception("Pruning failed")
        await asyncio.sleep(12 * 60 * 60)


async def async_main(*, address, port):
    engine = hwci.target.ci.engine_from_config_toml()
    ENGINE.set(engine)

    app = web.Application()
    app.add_routes(routes)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, address, port)
    await site.start()

    async with asyncio.TaskGroup() as tg:
        tg.create_task(engine.run())
        tg.create_task(prune_periodically(engine))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--address", type=str, default="localhost")
    parser.add_argument("--port", type=int, default=10898)
    parser.add_argument("--mock-devices", action="store_true")

    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG)

    if args.mock_devices:
        hwci.target.ci.mock_devices = True

    asyncio.run(async_main(address=args.address, port=args.port))
