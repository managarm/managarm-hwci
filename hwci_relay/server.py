from aiohttp import web
import argparse
import asyncio
import contextvars
import logging
import pydantic
import typing

import hwci_relay.ci

ENGINE = contextvars.ContextVar("hwci_relay.server.ENGINE")


class RunRequestData(pydantic.BaseModel):
    device: str
    timeout: float = 5
    tftp: typing.Dict[str, str]


async def post_file(request):
    hdigest = request.match_info["hdigest"]
    data = await request.content.read()

    engine = ENGINE.get()
    engine.cas.write_object(hdigest, data)

    return web.Response(text="OK")


async def post_run(request):
    req_data = RunRequestData.model_validate(await request.json())

    response = web.StreamResponse()
    await response.prepare(request)

    engine = ENGINE.get()
    device = engine.get_device(req_data.device)
    run = hwci_relay.ci.Run(
        engine, device, tftp=req_data.tftp, timeout=req_data.timeout
    )
    run.submit()

    async for buf in run.iter_logs():
        await response.write(buf)
    await response.write_eof()

    return response


async def async_main(*, address, port):
    engine = hwci_relay.ci.engine_from_config_toml()
    ENGINE.set(engine)

    app = web.Application()
    app.add_routes(
        [
            web.post("/run", post_run),
            web.post("/file/{hdigest}", post_file),
        ]
    )

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, address, port)
    await site.start()

    await engine.run()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--address", type=str, default="localhost")
    parser.add_argument("--port", type=int, default=10899)
    parser.add_argument("--mock-targets", action="store_true")

    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG)

    if args.mock_targets:
        hwci_relay.ci.mock_targets = True

    asyncio.run(async_main(address=args.address, port=args.port))
