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
import typing
import urllib.parse
from urllib.parse import urljoin

import hwci.boot_artifacts
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


async def upload_object(hdigest, obj, *, session, relay, token):
    response = await session.post(
        urljoin(f"{relay}/", f"file/{hdigest}"),
        headers={
            "Authorization": f"Bearer {token}",
        },
        data=hwci_cas.serialize(obj),
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

        await hwci.boot_artifacts.generate_tftp(
            out=tftpdir,
            profile=preset.profile,
            sysroot=sysroot,
        )

        tftp_files = list(walk_regular(tftpdir))
        tftp_objs = {
            path: hwci_cas.Object.make_blob(read_file(os.path.join(tftpdir, path)))
            for path in tftp_files
        }
        tftp_hdigests = {path: obj.hdigest() for path, obj in tftp_objs.items()}

        async with asyncio.TaskGroup() as tg:
            for path in tftp_files:
                obj = tftp_objs[path]
                hdigest = tftp_hdigests[path]
                print(f"Uploading {path} ({hdigest})")
                tg.create_task(
                    upload_object(
                        hdigest, obj, session=session, relay=relay, token=token
                    )
                )

    print("Running hwci")

    response = await session.post(
        urljoin(f"{relay}/", "run"),
        headers={
            "Authorization": f"Bearer {token}",
        },
        json={
            "device": "rpi4",
            "tftp": tftp_hdigests,
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


async def async_main(cfg, preset, *, relay):
    # Load secret_tokens.json.
    try:
        with open(os.path.join(state_home, "secret_tokens.json")) as f:
            secret_tokens_json = json.load(f)
    except FileNotFoundError:
        pass
    else:
        secret_tokens.root = SecretTokenDict.model_validate(secret_tokens_json).root

    async with aiohttp.ClientSession() as session:
        if relay not in secret_tokens.root:
            await authenticate(cfg, session=session, relay=relay)

        token = secret_tokens.root[relay].token
        await run(cfg, preset, session=session, relay=relay, token=token)


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

    args = parser.parse_args()

    # Normalize the relay URL to ensure that we find the right entry in secret_tokens.json.
    relay_url = normalize_api_url(args.relay)

    with open("hwci.toml", "rb") as f:
        cfg_toml = tomllib.load(f)
    cfg = Config.model_validate(cfg_toml)

    asyncio.run(async_main(cfg, "rpi4", relay=relay_url))
