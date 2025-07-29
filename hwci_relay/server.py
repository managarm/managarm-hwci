from aiohttp import web
import argparse
import asyncio
import contextvars
import logging
import pydantic
import re
import typing

import hwci_cas
import hwci_relay.auth
import hwci_relay.ci

logger = logging.getLogger(__name__)

AUTH = contextvars.ContextVar("hwci_relay.server.AUTH")
ENGINE = contextvars.ContextVar("hwci_relay.server.ENGINE")

routes = web.RouteTableDef()


class RunRequestData(pydantic.BaseModel):
    device: str
    timeout: float = 5
    tftp: typing.Dict[str, str]


@routes.get("/auth/nonce", name="get_auth_nonce")
async def get_auth_nonce(request):
    auth = AUTH.get()
    return web.Response(text=auth.nonce())


@routes.post("/auth/ssh_key", name="post_auth_sshkey")
async def post_auth_sshkey(request):
    data = await request.json()

    auth = AUTH.get()
    token = await auth.authenticate_by_ssh_key(data["nonce"], data["signature"])
    if token is None:
        raise web.HTTPUnauthorized()

    return web.Response(text=token)


async def post_file(request):
    hdigest = request.match_info["hdigest"]
    buf = await request.content.read()

    engine = ENGINE.get()
    engine.cas.write_object(hdigest, hwci_cas.deserialize(buf))

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


no_auth_routes = {
    "get_auth_nonce",
    "post_auth_sshkey",
}

# Valid characters for token from RFC 6750.
BEARER_RE = re.compile(r"^Bearer ([a-zA-Z0-9-._~+/=]+)$")


@web.middleware
async def auth_middleware(request, handler):
    route = request.match_info.route

    # Skip over routes that need to be accessible w/o authentication.
    if route.name in no_auth_routes:
        return await handler(request)

    authorization = request.headers.get("Authorization")
    if authorization is None:
        logger.warning("No authorization header")
        raise web.HTTPUnauthorized()

    match = BEARER_RE.fullmatch(authorization)
    if match is None:
        logger.warning("Authorization header does not conform to bearer scheme")
        raise web.HTTPUnauthorized()

    auth = AUTH.get()
    if not auth.validate_token(match.group(1)):
        logger.warning("Failed to validate bearer token")
        raise web.HTTPUnauthorized()

    return await handler(request)


async def async_main(*, address, port):
    auth = hwci_relay.auth.Auth()
    engine = hwci_relay.ci.engine_from_config_toml()
    AUTH.set(auth)
    ENGINE.set(engine)

    app = web.Application(middlewares=[auth_middleware])
    app.add_routes(routes)
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
