import aiohttp
import argparse
import asyncio
import contextlib
import json
import os
import pydantic
import shutil
import subprocess
import tempfile
import tomllib
import tqdm
import typing
import urllib.parse
import uuid
from urllib.parse import urljoin
import zstandard

import hwci.boot_artifacts
import hwci.timer_util
import hwci.xbps
import hwci.cas


class SecretToken(pydantic.BaseModel):
    token: str


SecretTokenDict = pydantic.RootModel[dict[str, SecretToken]]

secret_tokens = SecretTokenDict({})

# Directory that stores secret_tokens.json.
state_home = os.path.expanduser("~/.local/state/hwci")


def walk_regular(base_path, subdir=None):
    if subdir is None:
        scan_path = base_path
    else:
        scan_path = os.path.join(base_path, subdir)
    for entry in os.scandir(scan_path):
        if subdir is None:
            path = entry.name
        else:
            path = os.path.join(subdir, entry.name)
        if entry.is_dir(follow_symlinks=False):
            yield from walk_regular(base_path, path)
        elif entry.is_file(follow_symlinks=False):
            yield path


class PresetConfig(pydantic.BaseModel):
    arch: str
    repository: str
    packages: list[str]
    profile: str


class Config(pydantic.BaseModel):
    ssh_identity_file: str
    repositories: typing.Dict[str, str]
    presets: typing.Dict[str, PresetConfig]


def nbytes_human_readable(n):
    kib = 1024
    mib = 1024 * kib
    gib = 1024 * mib
    tib = 1024 * gib
    if n >= tib:
        return f"{n / tib:.2f} TiB"
    if n >= gib:
        return f"{n / gib:.2f} GiB"
    if n >= mib:
        return f"{n / mib:.2f} MiB"
    if n >= kib:
        return f"{n / kib:.2f} KiB"
    return f"{n} B"


class Run:
    __slots__ = (
        "device",
        "session",
        "relay",
        "token",
        "timeout",
        "_run_id",
        "_dissectors",
        "_objects",
        "_tftp",
        "_image",
        "_upload_nbytes",
    )

    def __init__(self, *, device, session, relay, token, timeout):
        self.device = device
        self.session = session
        self.relay = relay
        self.token = token
        self.timeout = timeout
        self._run_id = str(uuid.uuid4())
        self._dissectors = []
        # Maps hdigests to objects.
        self._objects = {}
        # Maps TFTP relative paths to hdigests.
        self._tftp = {}
        self._image = None
        self._upload_nbytes = 0

    async def run_from_repos(self, cfg, preset):
        preset = cfg.presets[preset]
        repo_name = preset.repository
        repo_url = cfg.repositories[repo_name]

        cache_dir = os.path.realpath(os.path.join("xbps-cache", repo_name))

        with tempfile.TemporaryDirectory(prefix="hwci-", dir=".") as rundir:
            sysroot = os.path.join(rundir, "sysroot")

            print(f"Preparing sysroot: {sysroot}")

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

            tftpdir = os.path.join(rundir, "tftp")
            await self._gen_tftp(preset.profile, sysroot=sysroot, tftpdir=tftpdir)
            with self._cleanup_dissectors():
                await self._dissect_tftp_dir(tftpdir)
                await self._new_run()
                await self._upload()
            await self._launch()

    async def run_from_sysroot(self, cfg, preset, sysroot, image_path):
        preset = cfg.presets[preset]

        with tempfile.TemporaryDirectory(prefix="hwci-", dir=".") as rundir:
            tftpdir = os.path.join(rundir, "tftp")
            await self._gen_tftp(preset.profile, sysroot=sysroot, tftpdir=tftpdir)
            with self._cleanup_dissectors():
                await self._dissect_tftp_dir(tftpdir)
                await self._dissect_image(image_path)
                await self._new_run()
                await self._upload()
            await self._launch()

    async def run_from_tftp(self, tftpdir):
        with self._cleanup_dissectors():
            await self._dissect_tftp_dir(tftpdir)
            await self._new_run()
            await self._upload()
        await self._launch()

    async def _gen_tftp(self, profile, *, sysroot, tftpdir):
        await hwci.boot_artifacts.generate_tftp(
            out=tftpdir,
            profile=profile,
            sysroot=sysroot,
        )

    async def _dissect_tftp_dir(self, tftpdir):
        with hwci.timer_util.Timer() as dissect_timer:
            tftp_files = list(walk_regular(tftpdir))
            for relpath in tftp_files:
                path = os.path.join(tftpdir, relpath)
                with open(path, "rb") as f:
                    dissector = hwci.cas.Dissector(f)
                    self._dissectors.append(dissector)
                    hdigest = await dissector.dissect_into(object_dict=self._objects)
                self._tftp[relpath] = hdigest
        print(f"Dissected files in {dissect_timer.elapsed:.2f} s")

    async def _dissect_image(self, image_path):
        if image_path is None:
            return
        with hwci.timer_util.Timer() as dissect_timer:
            with open(image_path, "rb") as f:
                dissector = hwci.cas.Dissector(f)
                self._dissectors.append(dissector)
                hdigest = await dissector.dissect_into(object_dict=self._objects)
                self._image = hdigest
        print(f"Dissected image in {dissect_timer.elapsed:.2f} s")

    @contextlib.contextmanager
    def _cleanup_dissectors(self):
        yield
        for dissector in self._dissectors:
            dissector.close()

    async def _new_run(self):
        response = await self.session.post(
            urljoin(f"{self.relay}/", "runs"),
            headers={
                "Authorization": f"Bearer {self.token}",
            },
            json={
                "run_id": self._run_id,
                "device": self.device,
                "tftp": self._tftp,
                "image": self._image,
                "timeout": self.timeout,
            },
        )
        response.raise_for_status()

    async def _upload(self):
        print("Files:")
        for relpath, hdigest in self._tftp.items():
            print(f"    tftp: {relpath} ({hdigest})")

        with hwci.timer_util.Timer() as upload_timer:
            while True:
                missing_on_relay = await self._get_missing_on_relay()
                if not missing_on_relay:
                    break
                print(f"Relay is missing {len(missing_on_relay)} objects")

                queue = missing_on_relay
                with tqdm.tqdm(total=len(missing_on_relay), unit="objs") as pbar:
                    semaphore = asyncio.Semaphore(4)
                    async with asyncio.TaskGroup() as tg:
                        while queue:
                            await semaphore.acquire()
                            objects = self._group_objects_for_upload(queue)
                            task = tg.create_task(
                                self._upload_objects(objects, pbar=pbar)
                            )
                            task.add_done_callback(lambda task: semaphore.release())

        nbytes_per_s = self._upload_nbytes / upload_timer.elapsed
        print(
            f"Uploaded files in {upload_timer.elapsed:.2f} s"
            f" ({nbytes_human_readable(self._upload_nbytes)} in total,"
            f" {nbytes_human_readable(nbytes_per_s)}/s)"
        )

    async def _launch(self):
        print("Launching run")

        response = await self.session.post(
            urljoin(f"{self.relay}/", f"runs/{self._run_id}/launch"),
            headers={
                "Authorization": f"Bearer {self.token}",
            },
        )
        response.raise_for_status()

        async with self.session.ws_connect(
            urljoin(f"{self.relay}/", f"runs/{self._run_id}/console"),
            headers={"Authorization": f"Bearer {self.token}"},
        ) as ws:
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    print(data["chunk"], end="", flush=True)
                else:
                    raise RuntimeError(
                        f"Unexpected message type {msg.type} on WebSocket"
                    )

    def _group_objects_for_upload(self, queue):
        chunk = {}
        n = 0
        while queue:
            hdigest = queue.pop()
            obj = self._objects[hdigest]
            chunk[hdigest] = obj
            n += len(obj.data)
            if n > 2 * 1024 * 1024:
                break
        return chunk

    async def _get_missing_on_relay(self):
        response = await self.session.get(
            urljoin(f"{self.relay}/", f"runs/{self._run_id}/missing"),
            headers={
                "Authorization": f"Bearer {self.token}",
            },
            raise_for_status=True,
        )
        return await response.json()

    async def _upload_objects(self, objects, *, pbar):
        compressor = zstandard.ZstdCompressor(level=-1)
        serializer = hwci.cas.Serializer()
        for hdigest, obj in objects.items():
            objbuf = obj.to_object_buffer().to_compressed(compressor=compressor)
            digest = bytes.fromhex(hdigest)
            serializer.serialize(digest, objbuf)
            self._upload_nbytes += len(objbuf.buffer)
        await self.session.post(
            urljoin(f"{self.relay}/", f"runs/{self._run_id}/files"),
            headers={
                "Authorization": f"Bearer {self.token}",
            },
            data=serializer.buf,
            raise_for_status=True,
        )
        pbar.update(len(objects))


async def authenticate(cfg, *, session, relay):
    nonce_response = await session.get(
        urljoin(f"{relay}/", "auth/nonce"),
        raise_for_status=True,
    )
    nonce = await nonce_response.text()

    process = await asyncio.create_subprocess_exec(
        "ssh-keygen",
        "-Y",
        "sign",
        "-f",
        os.path.expanduser(cfg.ssh_identity_file),
        "-nhwci",
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    (stdout, stderr) = await process.communicate(nonce.encode("ascii"))
    if process.returncode != 0:
        raise RuntimeError(
            f"ssh-keygen -Y sign failed with status {process.returncode}"
        )
    signature = stdout.decode("ascii")

    authenticate_response = await session.post(
        urljoin(f"{relay}/", "auth/ssh_key"),
        json={
            "nonce": nonce,
            "signature": signature,
        },
        raise_for_status=True,
    )
    token = await authenticate_response.text()

    secret_tokens.root[relay] = SecretToken(token=token)

    # Write-back secret_tokens.json
    secret_tokens_json = json.dumps(secret_tokens.model_dump())
    os.makedirs(state_home, exist_ok=True)
    with open(os.path.join(state_home, "secret_tokens.json"), "w") as f:
        f.write(secret_tokens_json)


async def async_run_from_repos(cfg, device, preset, *, relay, timeout):
    async with aiohttp.ClientSession() as session:
        if relay not in secret_tokens.root:
            await authenticate(cfg, session=session, relay=relay)

        token = secret_tokens.root[relay].token
        run = Run(
            device=device, session=session, relay=relay, token=token, timeout=timeout
        )
        await run.run_from_repos(cfg, preset)


async def async_run_from_sysroot(
    cfg, device, preset, *, sysroot, image_path, relay, timeout
):
    async with aiohttp.ClientSession() as session:
        if relay not in secret_tokens.root:
            await authenticate(cfg, session=session, relay=relay)

        token = secret_tokens.root[relay].token
        run = Run(
            device=device, session=session, relay=relay, token=token, timeout=timeout
        )
        await run.run_from_sysroot(cfg, preset, sysroot, image_path)


async def async_run_from_tftp(cfg, device, tftpdir, *, relay, timeout):
    async with aiohttp.ClientSession() as session:
        if relay not in secret_tokens.root:
            await authenticate(cfg, session=session, relay=relay)

        token = secret_tokens.root[relay].token
        run = Run(
            device=device, session=session, relay=relay, token=token, timeout=timeout
        )
        await run.run_from_tftp(tftpdir)


def normalize_api_url(url):
    parts = urllib.parse.urlparse(url)
    if parts.scheme not in {"http", "https"}:
        raise ValueError("Relay URL requires http:// or https://")
    # urljoin() normalizes the path to be absolute.
    return urljoin(
        urllib.parse.urlunparse(
            # Ensure that the last component is interpreted as a directory.
            # Otherwise, urljoin() will strip it.
            parts._replace(path=f"{parts.path}/")
        ),
        ".",
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--relay", type=str, required=True)
    parser.add_argument("-d", "--device", type=str, required=True)
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--image", type=str)

    run_mutgrp = parser.add_mutually_exclusive_group(required=True)
    run_mutgrp.add_argument("--repo", action="store_true")
    run_mutgrp.add_argument("--sysroot", type=str)
    run_mutgrp.add_argument("--tftp", type=str)

    args = parser.parse_args()

    # Normalize the relay URL to ensure that we find the right entry in secret_tokens.json.
    relay_url = normalize_api_url(args.relay)

    with open("hwci.toml", "rb") as f:
        cfg_toml = tomllib.load(f)
    cfg = Config.model_validate(cfg_toml)

    # Load secret_tokens.json.
    try:
        with open(os.path.join(state_home, "secret_tokens.json")) as f:
            secret_tokens_json = json.load(f)
    except FileNotFoundError:
        pass
    else:
        secret_tokens.root = SecretTokenDict.model_validate(secret_tokens_json).root

    if args.repo:
        asyncio.run(
            async_run_from_repos(
                cfg,
                args.device,
                preset=args.device,
                relay=relay_url,
                timeout=args.timeout,
            )
        )
    elif args.sysroot:
        asyncio.run(
            async_run_from_sysroot(
                cfg,
                args.device,
                preset=args.device,
                sysroot=args.sysroot,
                image_path=args.image,
                relay=relay_url,
                timeout=args.timeout,
            )
        )
    elif args.tftp:
        asyncio.run(
            async_run_from_tftp(
                cfg,
                args.device,
                tftpdir=args.tftp,
                relay=relay_url,
                timeout=args.timeout,
            )
        )
