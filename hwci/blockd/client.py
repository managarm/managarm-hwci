import asyncio
import logging

import hwci.blockd.models

logger = logging.getLogger(__name__)


class Client:
    __slots__ = (
        "_reader",
        "_writer",
    )

    def __init__(self):
        self._reader = None
        self._writer = None

    async def setup(self, nqn, backing_file, *, snapshot):
        await self._connect()

        command = hwci.blockd.models.SetupCommand(
            backing_file=backing_file,
            nqn=nqn,
            snapshot=snapshot,
        )
        cmdbuf = command.model_dump_json().encode("utf-8")

        self._writer.write(len(cmdbuf).to_bytes(4, "little"))
        self._writer.write(cmdbuf)
        await self._writer.drain()

        stlen = int.from_bytes(await self._reader.readexactly(4), "little")
        status = hwci.blockd.models.Status.model_validate_json(
            await self._reader.readexactly(stlen)
        )
        logger.info("Status from blockd: %s", status)

    async def close(self):
        if self._writer:
            self._writer.close()
            await self._writer.wait_closed()

    async def _connect(self):
        (reader, writer) = await asyncio.open_unix_connection("/run/hwci-blockd.sock")
        self._reader = reader
        self._writer = writer
