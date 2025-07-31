from aiohttp import web
import argparse
import asyncio
import contextvars
import logging
import pydantic
import typing

import hwci_cas
import hwci_target.ci

logger = logging.getLogger(__name__)

ENGINE = contextvars.ContextVar("hwci_target.server.ENGINE")

routes = web.RouteTableDef()


class NewRunData(pydantic.BaseModel):
    run_id: str
    device: str
    tftp: typing.Dict[str, str]
    timeout: int


@routes.post("/files")
async def post_files(request):
    multipart = await request.multipart()

    engine = ENGINE.get()
    n = 0
    writes = []
    while True:
        part = await multipart.next()
        if part is None:
            break
        hdigest = part.filename
        buf = bytes(await part.read())
        writes.append((hdigest, hwci_cas.deserialize(buf)))
        n += 1
    engine.cas.write_many_objects(writes)
    logger.info("Received %d objects", n)

    return web.Response(text="OK")


@routes.post("/runs")
async def post_runs(request):
    engine = ENGINE.get()
    data = NewRunData.model_validate(await request.json())

    device = engine.get_device(data.device)
    engine.new_run(data.run_id, device, tftp=data.tftp, timeout=data.timeout)

    return web.Response(text="OK")


@routes.post("/runs/{run_id}/launch")
async def post_launch(request):
    engine = ENGINE.get()
    run_id = request.match_info["run_id"]

    run = engine.get_run(run_id)
    run.submit()

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


async def async_main(*, address, port):
    engine = hwci_target.ci.engine_from_config_toml()
    ENGINE.set(engine)

    app = web.Application()
    app.add_routes(routes)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, address, port)
    await site.start()

    await engine.run()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--address", type=str, default="localhost")
    parser.add_argument("--port", type=int, default=10898)
    parser.add_argument("--mock-devices", action="store_true")

    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG)

    if args.mock_devices:
        hwci_target.ci.mock_devices = True

    asyncio.run(async_main(address=args.address, port=args.port))
