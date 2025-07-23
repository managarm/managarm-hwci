import aiohttp
import asyncio
import os
import pydantic
import termios
import tomllib
import tty
import typing

import hwci.aio
import hwci.shelly


class SwitchConfig(pydantic.BaseModel):
    shelly: str


class DeviceConfig(pydantic.BaseModel):
    uart: str
    switch: SwitchConfig


class Config(pydantic.BaseModel):
    devices: typing.Dict[str, DeviceConfig]


class Engine:
    __slots__ = (
        "_devices",
        "_q",
        "_http_client",
    )

    @classmethod
    def load(Cls, cfg):
        self = Cls()
        for k, v in cfg.devices.items():
            self._devices[k] = Device.load(self, v)
        return self

    def __init__(self):
        self._devices = {}
        self._q = asyncio.Queue()
        self._http_client = aiohttp.ClientSession()

    def get_device(self, name):
        return self._devices[name]

    async def run(self):
        while True:
            run = await self._q.get()
            await run._dispatch()


class Device:
    __slots__ = (
        "engine",
        "_uart_path",
        "_switch",
    )

    @classmethod
    def load(Cls, engine, cfg):
        self = Cls(engine)
        self._uart_path = cfg.uart
        self._switch = hwci.shelly.Switch(engine._http_client, cfg.switch.shelly)
        return self

    def __init__(self, engine):
        self.engine = engine

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

    async def _supervise(self, uart_task):
        await self.device._switch.flip_on()

        await asyncio.sleep(self.timeout)

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
    return Engine.load(cfg)
