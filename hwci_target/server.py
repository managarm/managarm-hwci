from aiohttp import web
import argparse
import asyncio
import contextvars
import logging
import pydantic

import hwci_target.ci

ENGINE = contextvars.ContextVar("hwci_target.server.ENGINE")


class RunRequestData(pydantic.BaseModel):
    device: str
    timeout: float = 5


async def post_run(request):
    req_data = RunRequestData.model_validate(await request.json())

    response = web.StreamResponse()
    await response.prepare(request)

    engine = ENGINE.get()
    device = engine.get_device(req_data.device)
    run = hwci_target.ci.Run(engine, device, timeout=req_data.timeout)
    run.submit()

    async for buf in run.iter_logs():
        await response.write(buf)
    await response.write_eof()

    return response


async def async_main():
    engine = hwci_target.ci.engine_from_config_toml()
    ENGINE.set(engine)

    app = web.Application()
    app.add_routes([web.post("/run", post_run)])

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "localhost", 1234)
    await site.start()

    await engine.run()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mock-devices", action="store_true")

    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG)

    if args.mock_devices:
        hwci_target.ci.mock_devices = True

    asyncio.run(async_main())
