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
    digests = []
    while True:
        item = deserializer.deserialize()
        if item is None:
            break
        digest, objbuf = item
        writes.append((digest, objbuf))
        digests.append(digest)

    engine.cas.write_many_object_buffers(writes)
    logger.info("Received %d objects", len(writes))

    run.notify_objects(digests)

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


@routes.get("/runs/{run_id}/updates")
async def get_updates(request):
    engine = ENGINE.get()
    run_id = request.match_info["run_id"]
    run = engine.get_run(run_id)

    # Sending a heartbeat is cheaper than using a receive timeout.
    # Hence, we rely on a server-to-client heartbeat to check responsiveness.
    ws = web.WebSocketResponse(heartbeat=20)
    await ws.prepare(request)

    async def forward_logs():
        try:
            async for buf in run.iter_logs():
                await ws.send_bytes(buf)
        finally:
            await ws.close()

    async with asyncio.TaskGroup() as tg:
        tg.create_task(forward_logs())

        # We can only read from the WS inside this task (due to aiohttp limitations).
        async for msg in ws:
            if msg.type == web.WSMsgType.ERROR:
                raise RuntimeError(f"Received websocket error: {msg.data}")
            else:
                raise RuntimeError(f"Unexpected message type {msg.type} on WebSocket")

    return ws


async def prune_periodically(engine):
    while True:
        try:
            keep_digests = set()
            for run in engine.get_all_runs():
                keep_digests.update(run.get_root_objects())
            engine.cas.prune(keep_digests)
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
