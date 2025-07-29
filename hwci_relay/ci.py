import aiohttp
import asyncio
import logging
import pydantic
import tomllib
import typing
import uuid

import hwci_cas

logger = logging.getLogger(__name__)

# Global flag that causes all requests to the target to be mocked.
mock_targets = False


class Config(pydantic.BaseModel):
    devices: typing.Dict[str, str]


class Engine:
    __slots__ = (
        "cfg",
        "cas",
        "_devices",
        "_q",
        "_http_client",
    )

    def __init__(self, cfg):
        self.cfg = cfg
        self.cas = hwci_cas.Store("relay/objects")
        self._devices = {}
        self._q = asyncio.Queue()
        self._http_client = aiohttp.ClientSession()

        for name, host in cfg.devices.items():
            self._devices[name] = Device(self, name, host)

    def get_device(self, name):
        return self._devices[name]

    async def run(self):
        while True:
            run = await self._q.get()
            try:
                await run._dispatch()
            except Exception as e:
                logger.error("Unhandled exception while dispatching run", exc_info=e)


class Device:
    __slots__ = (
        "engine",
        "name",
        "host",
    )

    def __init__(self, engine, name, host):
        self.engine = engine
        self.name = name
        self.host = host


class Run:
    __slots__ = (
        "engine",
        "device",
        "run_id",
        "tftp",
        "timeout",
        "_cond",
        "_done",
        "_logs",
    )

    def __init__(self, engine, device, *, tftp, timeout):
        self.engine = engine
        self.device = device
        self.run_id = str(uuid.uuid4())
        self.tftp = tftp
        self.timeout = timeout
        self._cond = asyncio.Condition()
        self._done = False
        self._logs = bytearray()

    def submit(self):
        self.engine._q.put_nowait(self)

    async def iter_logs(self):
        p = 0
        while True:
            buf = await self._read_log(p)
            if buf is None:
                return
            yield buf
            p += len(buf)

    async def _read_log(self, p):
        while True:
            async with self._cond:
                if p < len(self._logs):
                    return self._logs[p:]
                assert p == len(self._logs)
                if self._done:
                    return None
                await self._cond.wait()

    async def _dispatch(self):
        logger.info("Configuring run")
        await self._new_run()

        async with asyncio.TaskGroup() as tg:
            tg.create_task(self._supervise())
            tg.create_task(self._collect())

        async with self._cond:
            self._done = True
            self._cond.notify_all()

    async def _supervise(self):
        logger.info("Uploading objects")
        async with asyncio.TaskGroup() as tg:
            for hdigest in self.tftp.values():
                tg.create_task(self._upload(hdigest))

        logger.info("Launching run on %s", self.device.name)
        await self._launch()

    async def _collect(self):
        if mock_targets:
            return

        response = await self.engine._http_client.post(
            f"http://{self.device.host}:10898/runs/{self.run_id}/updates",
        )
        response.raise_for_status()

        while True:
            chunk = await response.content.read(4096)
            logger.info("received chunk: %d", len(chunk))
            if not chunk:
                break
            async with self._cond:
                self._logs += chunk
                self._cond.notify_all()

    async def _new_run(self):
        if mock_targets:
            return

        response = await self.engine._http_client.post(
            f"http://{self.device.host}:10898/runs",
            json={
                "run_id": self.run_id,
                "device": self.device.name,
                "tftp": self.tftp,
                "timeout": self.timeout,
            },
        )
        response.raise_for_status()

    async def _upload(self, hdigest):
        if mock_targets:
            return

        response = await self.engine._http_client.post(
            f"http://{self.device.host}:10898/file/{hdigest}",
            data=self.engine.cas.read_object(hdigest),
        )
        response.raise_for_status()

    async def _launch(self):
        if mock_targets:
            return

        response = await self.engine._http_client.post(
            f"http://{self.device.host}:10898/runs/{self.run_id}/launch",
        )
        response.raise_for_status()


def engine_from_config_toml():
    with open("relay.toml", "rb") as f:
        cfg_toml = tomllib.load(f)
    cfg = Config.model_validate(cfg_toml)
    return Engine(cfg)
