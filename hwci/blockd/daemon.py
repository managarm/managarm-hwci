import argparse
import asyncio
import json
import logging
import os
import shutil
import subprocess

import hwci.blockd.models as models

configfs_path = "/sys/kernel/config"

nvmet_path = os.path.join(configfs_path, "nvmet")

logger = logging.getLogger(__name__)


class BackingFile:
    __slots__ = ("name", "_path")

    def __init__(self, name, *, blockdir):
        self.name = name

        self._path = os.path.join(blockdir, name)

    @property
    def path(self):
        return self._path


class BlockDev:
    __slots__ = ("backing_file", "_loopdev")

    def __init__(self, backing_file):
        self.backing_file = backing_file
        self._loopdev = None

    @property
    def device_path(self):
        return self._loopdev

    def setup(self):
        process = subprocess.run(
            [
                "losetup",
                "-f",
                "--show",
                self.backing_file.path,
            ],
            check=True,
            stdout=subprocess.PIPE,
            text=True,
        )
        self._loopdev = process.stdout.strip()
        logger.info("Attached block device %s", self._loopdev)

    def shutdown(self):
        if self._loopdev:
            logger.info("Detaching block device %s", self._loopdev)
            subprocess.run(
                [
                    "losetup",
                    "-d",
                    self._loopdev,
                ],
                check=True,
            )
            self._loopdev = None


class NvmePort:
    __slots__ = ("port_id", "tcp_ip", "tcp_port", "_port_path")

    def __init__(self, *, tcp_ip, tcp_port=4420):
        self.port_id = 1
        self.tcp_ip = tcp_ip
        self.tcp_port = tcp_port

        self._port_path = os.path.join(nvmet_path, f"ports/{self.port_id}")

    def setup(self):
        os.mkdir(self._port_path)
        with open(os.path.join(self._port_path, "addr_trtype"), "w") as f:
            f.write("tcp")
        with open(os.path.join(self._port_path, "addr_traddr"), "w") as f:
            f.write(self.tcp_ip)
        with open(os.path.join(self._port_path, "addr_trsvcid"), "w") as f:
            f.write(str(self.tcp_port))
        with open(os.path.join(self._port_path, "addr_adrfam"), "w") as f:
            f.write("ipv4")

    def shutdown(self):
        os.rmdir(self._port_path)

    def nqn_link_path(self, nqn):
        return os.path.join(self._port_path, f"subsystems/{nqn}")


class NvmeSubsystem:
    __slots__ = ("nqn", "blockdev", "port", "_nqn_path", "_ns_path")

    def __init__(self, nqn, blockdev, port):
        self.nqn = nqn
        self.blockdev = blockdev
        self.port = port

        self._nqn_path = os.path.join(nvmet_path, f"subsystems/{self.nqn}")
        self._ns_path = os.path.join(self._nqn_path, "namespaces/1")

    def setup(self):
        logger.info("Setting up NVMe-oF NQN %s", self.nqn)
        # Set up the NQN.
        os.mkdir(self._nqn_path)
        with open(os.path.join(self._nqn_path, "attr_allow_any_host"), "w") as f:
            f.write("1")

        # Set up the NVMe namespace.
        os.mkdir(self._ns_path)
        with open(os.path.join(self._ns_path, "device_path"), "w") as f:
            f.write(self.blockdev.device_path)
        with open(os.path.join(self._ns_path, "enable"), "w") as f:
            f.write("1")

        # Link the NQN to the port.
        os.symlink(self._nqn_path, self.port.nqn_link_path(self.nqn))

    def shutdown(self):
        logger.info("Shutting down NVMe-oF NQN %s", self.nqn)
        os.unlink(self.port.nqn_link_path(self.nqn))
        os.rmdir(self._ns_path)
        os.rmdir(self._nqn_path)


# Represents a single connected client.
class Connection:
    __slots__ = ("nvme_port", "_backing_file", "_blockdev", "_nvme_subsystem")

    def __init__(self, *, nvme_port):
        self.nvme_port = nvme_port
        self._backing_file = None
        self._blockdev = None
        self._nvme_subsystem = None

    def shutdown(self):
        if self._nvme_subsystem:
            self._nvme_subsystem.shutdown()
        if self._blockdev:
            self._blockdev.shutdown()

    async def handle_cmd(self, reader, writer, *, blockdir):
        cmdlen = int.from_bytes(await reader.readexactly(4), "little")
        command = models.AnyCommand.model_validate_json(
            await reader.readexactly(cmdlen)
        ).root
        print(f"Command: {command}")

        assert isinstance(command, models.SetupCommand)

        self._backing_file = BackingFile(command.backing_file, blockdir=blockdir)
        if not os.path.exists(self._backing_file.path):
            raise RuntimeError(f"Backing file {command.backing_file} does not exist")

        self._blockdev = BlockDev(self._backing_file)
        self._blockdev.setup()

        self._nvme_subsystem = NvmeSubsystem(
            command.nqn, self._blockdev, self.nvme_port
        )
        self._nvme_subsystem.setup()

        logger.info("NVMe-of target is online")

        # Send status to the client.
        status = {}
        stbuf = json.dumps(status).encode("utf-8")
        writer.write(len(stbuf).to_bytes(4, "little"))
        writer.write(stbuf)
        await writer.drain()


async def handle_client(reader, writer, *, blockdir, nvme_port):
    try:
        conn = Connection(nvme_port=nvme_port)
        try:
            await conn.handle_cmd(reader, writer, blockdir=blockdir)
            # Wait until shutdown.
            await reader.read()
        except Exception:
            logger.exception("Exception in client handling")
        conn.shutdown()
        logger.info("Connection shutdown successful")
    finally:
        # We need to close the socket, otherwise the server will not terminate.
        writer.close()
        await writer.wait_closed()


def modprobe():
    subprocess.run(
        [
            "modprobe",
            "nvmet-tcp",
        ],
        check=True,
    )


async def run_daemon(*, blockdir, user, group, ip):
    modprobe()

    nvme_port = NvmePort(tcp_ip=ip)
    nvme_port.setup()

    try:
        async with asyncio.TaskGroup() as tg:
            path = "/run/hwci-blockd.sock"
            server = await asyncio.start_unix_server(
                lambda r, w: tg.create_task(
                    handle_client(r, w, blockdir=blockdir, nvme_port=nvme_port)
                ),
                path,
            )
            shutil.chown(path, user, group)
            os.chmod(path, 0o660)
            await server.serve_forever()
    finally:
        nvme_port.shutdown()


def main():
    logging.basicConfig(level=logging.DEBUG)

    parser = argparse.ArgumentParser()
    parser.add_argument("--user", type=str)
    parser.add_argument("--group", type=str)
    parser.add_argument("--ip", type=str, default="127.0.0.1")
    parser.add_argument("blockdir", type=str)

    args = parser.parse_args()

    asyncio.run(
        run_daemon(blockdir=args.blockdir, user=args.user, group=args.group, ip=args.ip)
    )
