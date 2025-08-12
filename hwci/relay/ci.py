import aiohttp
import asyncio
import logging
import os
import pydantic
import tomllib
import typing
import uuid

import hwci.cas
import hwci.timer_util

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
        "_runs",
        "_q",
        "_http_client",
    )

    def __init__(self, cfg):
        self.cfg = cfg
        self.cas = hwci.cas.Store("relay_objects")
        self._devices = {}
        self._runs = {}
        self._q = asyncio.Queue()
        self._http_client = aiohttp.ClientSession()

        for name, host in cfg.devices.items():
            self._devices[name] = Device(self, name, host)

    def get_device(self, name):
        return self._devices[name]

    def new_run(self, run_id, device, *, tftp, image, timeout):
        self._runs[run_id] = Run(self, device, tftp=tftp, image=image, timeout=timeout)

    def get_run(self, run_id):
        return self._runs[run_id]

    def get_all_runs(self):
        return self._runs.values()

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
        "image",
        "timeout",
        "_object_set",
        "_missing_set",
        "_cond",
        "_done",
        "_logs",
        "_retrieve_timer",
        "_transfer_timer",
        "_terminate_event",
    )

    def __init__(self, engine, device, *, tftp, image, timeout):
        self.engine = engine
        self.device = device
        self.run_id = str(uuid.uuid4())
        self.tftp = tftp
        self.image = image
        self.timeout = timeout
        self._object_set = set()
        self._missing_set = set()
        self._cond = asyncio.Condition()
        self._done = False
        self._logs = bytearray()
        self._retrieve_timer = hwci.timer_util.Timer()
        self._transfer_timer = hwci.timer_util.Timer()
        self._terminate_event = asyncio.Event()

        roots = list(self.get_root_objects())
        with hwci.timer_util.Timer() as walk_timer:
            for hdigest in roots:
                self.engine.cas.walk_tree_hdigests_into(
                    hdigest,
                    hdigest_set=self._object_set,
                    missing_set=self._missing_set,
                )
        logger.debug(
            "Walking %d trees took %.2f s (objects: %d, missing: %d)",
            len(roots),
            walk_timer.elapsed,
            len(self._object_set),
            len(self._missing_set),
        )

    def get_root_objects(self):
        yield from self.tftp.values()
        if self.image:
            yield self.image

    def missing_objects(self):
        return list(self._missing_set)

    def notify_objects(self, new_hdigests):
        self._missing_set.difference_update(new_hdigests)

        with hwci.timer_util.Timer() as walk_timer:
            for hdigest in new_hdigests:
                self.engine.cas.walk_tree_hdigests_into(
                    hdigest,
                    hdigest_set=self._object_set,
                    missing_set=self._missing_set,
                )
        logger.debug(
            "Walking %d trees took %.2f s (objects: %d, missing: %d)",
            len(new_hdigests),
            walk_timer.elapsed,
            len(self._object_set),
            len(self._missing_set),
        )

    def submit(self):
        self.engine.cas.bump(self._object_set)
        self.engine._q.put_nowait(self)

    def terminate(self):
        self._terminate_event.set()

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
        nbytes = 0
        with hwci.timer_util.Timer() as upload_timer:
            while True:
                missing_on_target = await self._get_missing_on_target()
                if not missing_on_target:
                    break
                logger.debug("Target is missing %d objects", len(missing_on_target))

                queue = missing_on_target
                semaphore = asyncio.Semaphore(4)
                async with asyncio.TaskGroup() as tg:
                    while queue:
                        await semaphore.acquire()
                        with self._retrieve_timer:
                            objects = self._group_objects_for_upload(queue)
                        task = tg.create_task(self._upload(objects))
                        task.add_done_callback(lambda task: semaphore.release())
                        nbytes += sum(len(obj.data) for obj in objects.values())

        logger.debug(
            "Uploaded objects in %.2f s (retrieval: %.2f s, transfer: %.2f s, %.2f KiB)",
            upload_timer.elapsed,
            self._retrieve_timer.elapsed,
            self._transfer_timer.elapsed,
            nbytes / 1024,
        )

        logger.info("Launching run on %s", self.device.name)
        await self._launch()

        try:
            async with asyncio.timeout(self.timeout):
                await self._terminate_event.wait()
            logger.info("Terminating run on request")
        except TimeoutError:
            logger.info("Terminating run on timeout")

        await self._terminate()

    async def _collect(self):
        if mock_targets:
            return

        response = await self.engine._http_client.post(
            f"http://{self.device.host}:10898/runs/{self.run_id}/updates",
        )
        response.raise_for_status()

        while True:
            chunk = await response.content.read(4096)
            if not chunk:
                break
            async with self._cond:
                self._logs += chunk
                self._cond.notify_all()

    def _group_objects_for_upload(self, queue):
        chunk = {}
        n = 0
        while queue:
            hdigest = queue.pop()
            obj = self.engine.cas.read_object(hdigest)
            chunk[hdigest] = obj
            n += len(obj.data)
            if n > 128 * 1024 * 1024:
                break
        return chunk

    async def _new_run(self):
        if mock_targets:
            return

        response = await self.engine._http_client.post(
            f"http://{self.device.host}:10898/runs",
            json={
                "run_id": self.run_id,
                "device": self.device.name,
                "tftp": self.tftp,
                "image": self.image,
            },
        )
        response.raise_for_status()

    async def _get_missing_on_target(self):
        response = await self.engine._http_client.get(
            f"http://{self.device.host}:10898/runs/{self.run_id}/missing",
            raise_for_status=True,
        )
        return await response.json()

    async def _upload(self, objects):
        if mock_targets:
            return

        with hwci.timer_util.Timer() as timer:
            serializer = hwci.cas.Serializer()
            for hdigest, obj in objects.items():
                digest = bytes.fromhex(hdigest)
                serializer.serialize(digest, obj.to_object_buffer())
            response = await self.engine._http_client.post(
                f"http://{self.device.host}:10898/runs/{self.run_id}/files",
                data=serializer.buf,
            )
            response.raise_for_status()
        self._transfer_timer.elapsed += timer.elapsed

    async def _launch(self):
        if mock_targets:
            return

        response = await self.engine._http_client.post(
            f"http://{self.device.host}:10898/runs/{self.run_id}/launch",
        )
        response.raise_for_status()

    async def _terminate(self):
        if mock_targets:
            return

        logger.info("Terminating run on %s", self.device.name)
        response = await self.engine._http_client.post(
            f"http://{self.device.host}:10898/runs/{self.run_id}/terminate",
        )
        response.raise_for_status()


def engine_from_config_toml(*, confdir):
    with open(os.path.join(confdir, "relay.toml"), "rb") as f:
        cfg_toml = tomllib.load(f)
    cfg = Config.model_validate(cfg_toml)
    return Engine(cfg)
