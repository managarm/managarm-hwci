import aiohttp
import argparse
import asyncio
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
from urllib.parse import urljoin

import hwci.boot_artifacts
import hwci.timer_util
import hwci.xbps
import hwci_cas


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
    __slots__ = ("session", "relay", "token", "_objects", "_tftp")

    def __init__(self, *, session, relay, token):
        self.session = session
        self.relay = relay
        self.token = token
        # Maps hdigests to objects.
        self._objects = {}
        # Maps TFTP relative paths to hdigests.
        self._tftp = {}

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

            await self._gen_tftp(preset.profile, sysroot=sysroot, rundir=rundir)
            await self._launch()

    async def run_from_sysroot(self, cfg, preset, sysroot):
        preset = cfg.presets[preset]

        with tempfile.TemporaryDirectory(prefix="hwci-", dir=".") as rundir:
            await self._gen_tftp(preset.profile, sysroot=sysroot, rundir=rundir)
            await self._launch()

    async def _gen_tftp(self, profile, *, sysroot, rundir):
        tftpdir = os.path.join(rundir, "tftp")

        await hwci.boot_artifacts.generate_tftp(
            out=tftpdir,
            profile=profile,
            sysroot=sysroot,
        )

        with hwci.timer_util.Timer() as dissect_timer:
            tftp_files = list(walk_regular(tftpdir))
            for relpath in tftp_files:
                path = os.path.join(tftpdir, relpath)
                with open(path, "rb") as f:
                    dissector = hwci_cas.Dissector(f)
                    hdigest = await dissector.dissect_into(object_dict=self._objects)
                self._tftp[relpath] = hdigest
        print(f"Dissected files in {dissect_timer.elapsed:.2f} s")

    async def _launch(self):
        print("Files:")
        for relpath in self._tftp.keys():
            print(f"    tftp: {relpath}")

        with (
            hwci.timer_util.Timer() as upload_timer,
            tqdm.tqdm(total=len(self._objects)) as pbar,
        ):
            async with asyncio.TaskGroup() as tg:
                for objects in self._group_objects_for_upload():
                    tg.create_task(self._upload_objects(objects, pbar=pbar))
        print(f"Uploaded files in {upload_timer.elapsed:.2f} s")

        print("Running hwci")

        response = await self.session.post(
            urljoin(f"{self.relay}/", "run"),
            headers={
                "Authorization": f"Bearer {self.token}",
            },
            json={
                "device": "rpi4",
                "tftp": self._tftp,
                "timeout": 60,
            },
        )
        response.raise_for_status()

        while True:
            chunk = await response.content.read(4096)
            if not chunk:
                break
            # TODO: This will fail if the chunk ends in the middle of a UTF-8 sequence.
            #       We should use a structured protocol (e.g., NULL delimited JSON) to avoid this.
            string = chunk.decode("utf-8")
            print(string, end="", flush=True)

    def _group_objects_for_upload(self):
        chunk = {}
        n = 0
        for hdigest, obj in self._objects.items():
            chunk[hdigest] = obj
            n += len(obj.data)
            if n > 2 * 1024 * 1024:
                yield chunk
                chunk = {}
                n = 0
        if chunk:
            yield chunk

    async def _upload_objects(self, objects, *, pbar):
        form_data = aiohttp.FormData()
        for hdigest, obj in objects.items():
            form_data.add_field("file", hwci_cas.serialize(obj), filename=hdigest)
        await self.session.post(
            urljoin(f"{self.relay}/", "files"),
            headers={
                "Authorization": f"Bearer {self.token}",
            },
            data=form_data,
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


async def async_run_from_repos(cfg, preset, *, relay):
    async with aiohttp.ClientSession() as session:
        if relay not in secret_tokens.root:
            await authenticate(cfg, session=session, relay=relay)

        token = secret_tokens.root[relay].token
        run = Run(session=session, relay=relay, token=token)
        await run.run_from_repos(cfg, preset)


async def async_run_from_sysroot(cfg, preset, sysroot, *, relay):
    async with aiohttp.ClientSession() as session:
        if relay not in secret_tokens.root:
            await authenticate(cfg, session=session, relay=relay)

        token = secret_tokens.root[relay].token
        run = Run(session=session, relay=relay, token=token)
        await run.run_from_sysroot(cfg, preset, sysroot)


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

    run_mutgrp = parser.add_mutually_exclusive_group(required=True)
    run_mutgrp.add_argument("--repo", action="store_true")
    run_mutgrp.add_argument("--sysroot", type=str)

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
        asyncio.run(async_run_from_repos(cfg, "rpi4", relay=relay_url))
    else:
        asyncio.run(async_run_from_sysroot(cfg, "rpi4", args.sysroot, relay=relay_url))
