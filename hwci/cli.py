import aiohttp
import argparse
import asyncio
import hashlib
import os
import pydantic
import shutil
import subprocess
import tempfile
import tomllib
import typing

import hwci.bootables
import hwci.xbps


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
    bootables: str


class Config(pydantic.BaseModel):
    ssh_identity_file: str
    repositories: typing.Dict[str, str]
    presets: typing.Dict[str, PresetConfig]


async def upload_object(sha256, blob, *, session, relay, token):
    response = await session.post(
        f"http://{relay}:10899/file/{sha256}",
        headers={
            "Authorization": f"Bearer {token}",
        },
        data=blob,
    )
    response.raise_for_status()


def read_file(path):
    with open(path, "rb") as f:
        return f.read()


async def run(cfg, preset, *, session, relay, token):
    preset = cfg.presets[preset]
    repo_name = preset.repository
    repo_url = cfg.repositories[repo_name]

    cache_dir = os.path.realpath(os.path.join("xbps-cache", repo_name))

    with tempfile.TemporaryDirectory(prefix="hwci-", dir=".") as rundir:
        sysroot = os.path.join(rundir, "sysroot")
        tftpdir = os.path.join(rundir, "tftp")

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

        await hwci.bootables.generate_tftp(
            out=tftpdir,
            profile=preset.bootables,
            sysroot=sysroot,
        )

        tftp_files = list(walk_regular(tftpdir))
        tftp_contents = {
            path: read_file(os.path.join(tftpdir, path)) for path in tftp_files
        }
        tftp_sha256 = {
            path: hashlib.sha256(contents).hexdigest()
            for path, contents in tftp_contents.items()
        }

        async with asyncio.TaskGroup() as tg:
            for path in tftp_files:
                blob = tftp_contents[path]
                sha256 = tftp_sha256[path]
                print(f"Uploading {path} ({sha256})")
                tg.create_task(
                    upload_object(
                        sha256, blob, session=session, relay=relay, token=token
                    )
                )

    print("Running hwci")

    response = await session.post(
        f"http://{relay}:10899/run",
        headers={
            "Authorization": f"Bearer {token}",
        },
        json={
            "device": "rpi4",
            "tftp": tftp_sha256,
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


async def authenticate(cfg, *, session, relay):
    nonce_response = await session.get(
        f"http://{relay}:10899/auth/nonce",
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
        f"http://{relay}:10899/auth/ssh_key",
        json={
            "nonce": nonce,
            "signature": signature,
        },
        raise_for_status=True,
    )
    token = await authenticate_response.text()

    with open("hwci.token", "w") as f:
        f.write(token)


async def async_main(cfg, preset, *, relay):
    async with aiohttp.ClientSession() as session:
        if not os.path.exists("hwci.token"):
            await authenticate(cfg, session=session, relay=relay)

        with open("hwci.token", "r") as f:
            token = f.read()
        await run(cfg, preset, session=session, relay=relay, token=token)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--relay", type=str, required=True)

    args = parser.parse_args()

    with open("hwci.toml", "rb") as f:
        cfg_toml = tomllib.load(f)
    cfg = Config.model_validate(cfg_toml)

    asyncio.run(async_main(cfg, "rpi4", relay=args.relay))
