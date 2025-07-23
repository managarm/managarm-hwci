from aiohttp import web
import asyncio
import contextvars
import pydantic

import hwci.ci

ENGINE = contextvars.ContextVar("hwci.Engine")


class RunRequestData(pydantic.BaseModel):
    device: str
    timeout: float = 5


async def post_run(request):
    req_data = RunRequestData.model_validate(await request.json())

    response = web.StreamResponse()
    await response.prepare(request)

    engine = ENGINE.get()
    device = engine.get_device(req_data.device)
    run = hwci.ci.Run(engine, device, timeout=req_data.timeout)
    run.submit()

    async for buf in run.iter_logs():
        await response.write(buf)
    await response.write_eof()

    return response


async def async_main():
    engine = hwci.ci.engine_from_config_toml()
    ENGINE.set(engine)

    app = web.Application()
    app.add_routes([web.post("/run", post_run)])

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "localhost", 1234)
    await site.start()

    await engine.run()


def main():
    asyncio.run(async_main())
