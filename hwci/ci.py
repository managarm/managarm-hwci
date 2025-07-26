import aiohttp
import asyncio
import logging
import os
import pydantic
import shutil
import tempfile
import termios
import tomllib
import tty
import typing

import hwci.aio
import hwci.bootables
import hwci.shelly
import hwci.xbps

logger = logging.getLogger(__name__)


class PresetConfig(pydantic.BaseModel):
    arch: str
    repository: str
    packages: list[str]
    bootables: str


class SwitchConfig(pydantic.BaseModel):
    shelly: str


class DeviceConfig(pydantic.BaseModel):
    uart: str
    switch: SwitchConfig


class Config(pydantic.BaseModel):
    devices: typing.Dict[str, DeviceConfig]
    repositories: typing.Dict[str, str]
    presets: typing.Dict[str, PresetConfig]


class Engine:
    __slots__ = (
        "cfg",
        "_devices",
        "_q",
        "_http_client",
    )

    def __init__(self, cfg):
        self.cfg = cfg
        self._devices = {}
        self._q = asyncio.Queue()
        self._http_client = aiohttp.ClientSession()

        for name, device_cfg in cfg.devices.items():
            self._devices[name] = Device(self, name, device_cfg)

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
        "cfg",
        "_uart_path",
        "_switch",
    )

    def __init__(self, engine, name, cfg):
        self.engine = engine
        self.name = name
        self.cfg = cfg
        self._uart_path = cfg.uart
        self._switch = hwci.shelly.Switch(engine._http_client, cfg.switch.shelly)

    def _setup_tty(self, fd):
        baud = termios.B115200

        # attrs is [iflag, oflag, cflag, lflag, ispeed, ospeed, cc].
        attrs = termios.tcgetattr(fd)
        assert len(attrs) == 7

        # Set raw mode and baud rate.
        tty.cfmakeraw(attrs)
        attrs[4] = baud  # ispeed
        attrs[5] = baud  # ospeed

        termios.tcsetattr(fd, termios.TCSANOW, attrs)


class Run:
    __slots__ = (
        "engine",
        "device",
        "timeout",
        "_cond",
        "_done",
        "_logs",
    )

    def __init__(self, engine, device, *, timeout=5):
        self.engine = engine
        self.device = device
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
        await self._prepare()

        await self.device._switch.ensure_off()

        uart = open(
            os.open(self.device._uart_path, os.O_RDWR | os.O_NOCTTY | os.O_NONBLOCK),
            "rb",
            buffering=0,
        )
        self.device._setup_tty(uart)

        # Discard data on the TTY.
        termios.tcflush(uart, termios.TCIOFLUSH)

        uart_task = asyncio.create_task(self._collect_uart(uart))
        supervise_task = asyncio.create_task(self._supervise(uart_task))

        await asyncio.gather(
            uart_task,
            supervise_task,
        )

    # Sets up the TFTP directory for this run.
    async def _prepare(self):
        preset = self.engine.cfg.presets[self.device.name]
        repo_name = preset.repository
        repo_url = self.engine.cfg.repositories[repo_name]

        cache_dir = os.path.realpath(os.path.join("xbps-cache", repo_name))

        with tempfile.TemporaryDirectory(prefix="sysroot-", dir=".") as sysroot:
            logger.info("Preparing sysroot: %s", sysroot)
            # Copy keys into the sysroot, otherwise xbps-install will ask for confirmation.
            key_dir = os.path.join(sysroot, "var/db/xbps/keys/")
            os.makedirs(key_dir, exist_ok=True)
            for file in os.listdir("xbps-keys"):
                shutil.copyfile(
                    os.path.join("xbps-keys", file),
                    os.path.join(key_dir, file),
                )

            await hwci.xbps.install(
                arch=preset.arch,
                pkgs=preset.packages,
                repo_url=repo_url,
                cache_dir=cache_dir,
                sysroot=sysroot,
            )

            await hwci.bootables.generate_tftp(
                out="/srv/tftp/",
                profile=preset.bootables,
                sysroot=sysroot,
            )

    async def _supervise(self, uart_task):
        logger.info("Switching %s ON", self.device.name)
        await self.device._switch.flip_on()

        await asyncio.sleep(self.timeout)

        logger.info("Switching %s OFF", self.device.name)
        await self.device._switch.flip_off()

        # Wait for the device to be truly off, then stop log collection.
        await asyncio.sleep(2)
        uart_task.cancel()

        async with self._cond:
            self._done = True
            self._cond.notify_all()

    async def _collect_uart(self, uart_file):
        with uart_file:
            reader = hwci.aio.UartReader(uart_file.fileno())
            while True:
                try:
                    buf = await reader.read()
                except asyncio.CancelledError:
                    break
                async with self._cond:
                    self._logs += buf
                    self._cond.notify_all()


def engine_from_config_toml():
    with open("config.toml", "rb") as f:
        cfg_toml = tomllib.load(f)
    cfg = Config.model_validate(cfg_toml)
    return Engine(cfg)
