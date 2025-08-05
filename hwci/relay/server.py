from aiohttp import web
import argparse
import asyncio
import contextvars
import logging
import pydantic
import re
import typing

import hwci.cas
import hwci.relay.auth
import hwci.relay.ci

logger = logging.getLogger(__name__)

AUTH = contextvars.ContextVar("hwci.relay.server.AUTH")
ENGINE = contextvars.ContextVar("hwci.relay.server.ENGINE")

routes = web.RouteTableDef()


class NewRunData(pydantic.BaseModel):
    run_id: str
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


@routes.post("/runs/{run_id}/files")
async def post_files(request):
    run_id = request.match_info["run_id"]
    multipart = await request.multipart()

    engine = ENGINE.get()
    run = engine.get_run(run_id)
    n = 0
    writes = []
    while True:
        part = await multipart.next()
        if part is None:
            break
        hdigest = part.filename
        buf = bytes(await part.read())
        writes.append((hdigest, hwci.cas.deserialize(buf)))
        n += 1
    engine.cas.write_many_objects(writes)
    logger.info("Received %d objects", n)

    run.notify_objects([hdigest for hdigest, obj in writes])

    return web.Response(text="OK")


@routes.post("/runs")
async def post_runs(request):
    engine = ENGINE.get()
    data = NewRunData.model_validate(await request.json())

    device = engine.get_device(data.device)
    engine.new_run(data.run_id, device, tftp=data.tftp, timeout=data.timeout)

    return web.Response(text="OK")


@routes.get("/runs/{run_id}/missing")
async def get_missing(request):
    run_id = request.match_info["run_id"]

    engine = ENGINE.get()
    run = engine.get_run(run_id)

    return web.json_response(run.missing_objects())


@routes.get("/runs/{run_id}/console")
async def get_console(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    engine = ENGINE.get()
    run_id = request.match_info["run_id"]
    run = engine.get_run(run_id)

    async for buf in run.iter_logs():
        await ws.send_json({"chunk": buf.decode("utf-8", errors="replace")})

    await ws.close()
    return ws


@routes.post("/runs/{run_id}/launch")
async def post_launch(request):
    engine = ENGINE.get()
    run_id = request.match_info["run_id"]

    run = engine.get_run(run_id)
    run.submit()

    return web.Response(text="OK")


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


async def async_main(*, confdir, address, port):
    auth = hwci.relay.auth.Auth(confdir=confdir)
    engine = hwci.relay.ci.engine_from_config_toml(confdir=confdir)
    AUTH.set(auth)
    ENGINE.set(engine)

    app = web.Application(middlewares=[auth_middleware])
    app.add_routes(routes)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, address, port)
    await site.start()

    await engine.run()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--confdir", type=str, default=".")
    parser.add_argument("--address", type=str, default="localhost")
    parser.add_argument("--port", type=int, default=10899)
    parser.add_argument("--mock-targets", action="store_true")

    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG)

    if args.mock_targets:
        hwci.relay.ci.mock_targets = True

    asyncio.run(async_main(confdir=args.confdir, address=args.address, port=args.port))
