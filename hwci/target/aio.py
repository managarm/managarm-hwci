import asyncio
import os


class UartReader:
    __slots__ = (
        "fd",
        "_loop",
        "_lock",
        "_readable",
    )

    def __init__(self, fd):
        self.fd = fd
        self._loop = asyncio.get_event_loop()
        self._lock = asyncio.Lock()

        self._readable = False

    async def read(self):
        async with self._lock:
            while True:
                # Wait until FD is readable.
                if not self._readable:
                    await self._wait_readable()
                    continue

                # Perform the read.
                try:
                    buf = os.read(self.fd, 4096)
                except BlockingIOError:
                    self._readable = False
                    continue

                return buf

    async def _wait_readable(self):
        assert not self._readable
        fut = self._loop.create_future()
        self._loop.add_reader(self.fd, fut.set_result, None)
        try:
            await fut
        finally:
            self._loop.remove_reader(self.fd)
        self._readable = True
