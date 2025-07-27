from aiohttp import web
import argparse
import asyncio
import contextvars
import hashlib
import logging
import os
import pydantic
import re
import tempfile
import typing

import hwci_target.ci

SHA256_RE = re.compile(r"^[a-f0-9]{64}$")

ENGINE = contextvars.ContextVar("hwci_target.server.ENGINE")


class RunRequestData(pydantic.BaseModel):
    device: str
    timeout: float = 5
    tftp: typing.Dict[str, str]


async def post_file(request):
    expected_sha256 = request.match_info["sha256"]
    if not SHA256_RE.fullmatch(expected_sha256):
        raise RuntimeError(f"Rejecting sha256 parameter: {expected_sha256}")

    committed = False
    os.makedirs("objects", exist_ok=True)
    (fd, path) = tempfile.mkstemp(dir="objects", prefix="upload-")
    try:
        with open(fd, "wb") as f:
            hasher = hashlib.sha256()
            while True:
                chunk = await request.content.read(16 * 1024)
                if not chunk:
                    break
                f.write(chunk)
                hasher.update(chunk)

        computed_sha256 = hasher.hexdigest()
        if computed_sha256 != expected_sha256:
            raise web.HTTPBadRequest(
                text=f"SHA256 mismatch, expected {expected_sha256}, got {computed_sha256}"
            )

        os.rename(path, os.path.join("objects", computed_sha256))
        committed = True
    finally:
        if not committed:
            os.unlink(path)

    return web.Response(text="OK")


async def post_run(request):
    req_data = RunRequestData.model_validate(await request.json())

    response = web.StreamResponse()
    await response.prepare(request)

    engine = ENGINE.get()
    device = engine.get_device(req_data.device)
    run = hwci_target.ci.Run(
        engine, device, tftp=req_data.tftp, timeout=req_data.timeout
    )
    run.submit()

    async for buf in run.iter_logs():
        await response.write(buf)
    await response.write_eof()

    return response


async def async_main(*, address, port):
    engine = hwci_target.ci.engine_from_config_toml()
    ENGINE.set(engine)

    app = web.Application()
    app.add_routes(
        [
            web.post("/run", post_run),
            web.post("/file/{sha256}", post_file),
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
    parser.add_argument("--port", type=int, default=10898)
    parser.add_argument("--mock-devices", action="store_true")

    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG)

    if args.mock_devices:
        hwci_target.ci.mock_devices = True

    asyncio.run(async_main(address=args.address, port=args.port))
