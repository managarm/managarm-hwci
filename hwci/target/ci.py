import aiohttp
import asyncio
import logging
import os
import pathlib
import pydantic
import termios
import tomllib
import typing

import hwci.timer_util
import hwci.blockd.client
import hwci.cas
import hwci.target.aio
import hwci.target.shelly

logger = logging.getLogger(__name__)

# Global flag that causes all device access (UART capturing + switch on/off)
# to be mocked. Useful for developing on a machine that has no devices attached.
mock_devices = False


class SwitchConfig(pydantic.BaseModel):
    shelly: str


class DeviceConfig(pydantic.BaseModel):
    tftp: str
    uart: str
    switch: SwitchConfig
    image: typing.Optional[str] = None


class Config(pydantic.BaseModel):
    devices: typing.Dict[str, DeviceConfig]


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
        self.cas = hwci.cas.Store("target_objects")
        self._devices = {}
        self._runs = {}
        self._q = asyncio.Queue()
        self._http_client = aiohttp.ClientSession()

        for name, device_cfg in cfg.devices.items():
            self._devices[name] = Device(self, name, device_cfg)

    def get_device(self, name):
        return self._devices[name]

    def new_run(self, run_id, device, *, tftp, image):
        self._runs[run_id] = Run(self, device, tftp=tftp, image=image)

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
            try:
                await run._shutdown()
            except Exception:
                logger.exception("Exception in run shutdown")


class Device:
    __slots__ = (
        "engine",
        "name",
        "cfg",
        "run",
        "_uart_path",
        "_switch",
    )

    def __init__(self, engine, name, cfg):
        self.engine = engine
        self.name = name
        self.cfg = cfg
        self.run = None
        self._uart_path = cfg.uart
        self._switch = hwci.target.shelly.Switch(engine._http_client, cfg.switch.shelly)

    def _setup_tty(self, fd):
        baud = termios.B115200

        # attrs is [iflag, oflag, cflag, lflag, ispeed, ospeed, cc].
        attrs = termios.tcgetattr(fd)
        assert len(attrs) == 7

        # Set raw mode.
        # This is adopted from the cfmakeraw() function in the tty module
        # of the Python's stdlib (which is only available at Python 3.12+).
        attrs[0] &= ~(
            termios.IGNBRK
            | termios.BRKINT
            | termios.IGNPAR
            | termios.PARMRK
            | termios.INPCK
            | termios.ISTRIP
            | termios.INLCR
            | termios.IGNCR
            | termios.ICRNL
            | termios.IXON
            | termios.IXANY
            | termios.IXOFF
        )
        attrs[1] &= ~termios.OPOST
        attrs[2] &= ~(termios.PARENB | termios.CSIZE)
        attrs[2] |= termios.CS8
        attrs[3] &= ~(
            termios.ECHO
            | termios.ECHOE
            | termios.ECHOK
            | termios.ECHONL
            | termios.ICANON
            | termios.IEXTEN
            | termios.ISIG
            | termios.NOFLSH
            | termios.TOSTOP
        )

        # Set baud rate.
        attrs[4] = baud  # ispeed
        attrs[5] = baud  # ospeed

        termios.tcsetattr(fd, termios.TCSANOW, attrs)


class Run:
    __slots__ = (
        "engine",
        "device",
        "tftp",
        "image",
        "_object_set",
        "_missing_set",
        "_blockd_client",
        "_cond",
        "_done",
        "_logs",
        "_terminate_event",
    )

    def __init__(self, engine, device, *, tftp, image):
        self.engine = engine
        self.device = device
        self.tftp = tftp
        self.image = image
        self._object_set = set()
        self._missing_set = set()
        self._blockd_client = None
        self._cond = asyncio.Condition()
        self._done = False
        self._logs = bytearray()
        self._terminate_event = asyncio.Event()

        roots = list(self.get_root_objects())
        with hwci.timer_util.Timer() as walk_timer:
            for hdigest in roots:
                self.engine.cas.walk_tree_digests_into(
                    hwci.cas.parse_hdigest(hdigest),
                    digest_set=self._object_set,
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

    def notify_objects(self, new_digests):
        self._missing_set.difference_update(new_digests)

        with hwci.timer_util.Timer() as walk_timer:
            for digest in new_digests:
                self.engine.cas.walk_tree_digests_into(
                    digest,
                    digest_set=self._object_set,
                    missing_set=self._missing_set,
                )
        logger.debug(
            "Walking %d trees took %.2f s (objects: %d, missing: %d)",
            len(new_digests),
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
        await self._prepare()

        if not mock_devices:
            await self.device._switch.ensure_off()

        if not mock_devices:
            uart_fd = os.open(
                self.device._uart_path, os.O_RDWR | os.O_NOCTTY | os.O_NONBLOCK
            )
        else:
            (mock_fd, uart_fd) = os.openpty()
            os.set_blocking(uart_fd, False)

        uart = open(uart_fd, "rb", buffering=0)
        self.device._setup_tty(uart)

        # Discard data on the TTY.
        termios.tcflush(uart, termios.TCIOFLUSH)

        if mock_devices:
            os.write(mock_fd, b"Hello world!")

        uart_task = asyncio.create_task(self._collect_uart(uart))
        supervise_task = asyncio.create_task(self._supervise(uart_task))

        await asyncio.gather(
            uart_task,
            supervise_task,
        )

        if mock_devices:
            os.close(mock_fd)

    async def _shutdown(self):
        if self._blockd_client:
            await self._blockd_client.close()

    # Sets up the TFTP directory for this run.
    async def _prepare(self):
        for path, hdigest in self.tftp.items():
            for part in pathlib.PurePath(path).parts:
                if not part or part == "." or part == "..":
                    raise RuntimeError(f"Path is rejected: {path}")
            self.engine.cas.extract(
                hwci.cas.parse_hdigest(hdigest),
                os.path.join(self.device.cfg.tftp, path),
            )

        if self.image:
            logger.info("Setting up image %s via blockd", self.device.cfg.image)
            with hwci.timer_util.Timer() as image_timer:
                with open(self.device.cfg.image, "r+b") as f:
                    self.engine.cas.extract_to(hwci.cas.parse_hdigest(self.image), f)
                    f.truncate()
            logger.debug("Wrote image in %.2f s", image_timer.elapsed)

            self._blockd_client = hwci.blockd.client.Client()
            await self._blockd_client.setup(
                "nqn.2024-12.org.managarm:nvme:managarm-boot",
                os.path.basename(self.device.cfg.image),
            )

    async def _supervise(self, uart_task):
        logger.info("Switching %s ON", self.device.name)
        if not mock_devices:
            await self.device._switch.flip_on()

        await self._terminate_event.wait()

        logger.info("Switching %s OFF", self.device.name)
        if not mock_devices:
            await self.device._switch.flip_off()

        # Wait for the device to be truly off, then stop log collection.
        await asyncio.sleep(2)
        uart_task.cancel()

        async with self._cond:
            self._done = True
            self._cond.notify_all()

    async def _collect_uart(self, uart_file):
        with uart_file:
            reader = hwci.target.aio.UartReader(uart_file.fileno())
            while True:
                try:
                    buf = await reader.read()
                except asyncio.CancelledError:
                    break
                async with self._cond:
                    self._logs += buf
                    self._cond.notify_all()


def engine_from_config_toml():
    with open("target.toml", "rb") as f:
        cfg_toml = tomllib.load(f)
    cfg = Config.model_validate(cfg_toml)
    return Engine(cfg)
