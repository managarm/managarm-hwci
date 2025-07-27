import asyncio
import os
import shlex
import subprocess


async def install(*, arch, pkgs, repo_url, cache_dir, sysroot):
    args = [
        # TODO: Assume xbps-install symlink in CWD for now.
        "./xbps-install",
        "-y",  # Yes.
        "-i",  # Ignore default repositories.
        "-c",  # Cache directory.
        cache_dir,
        "-S",  # Sync.
        "-u",  # Update
        "-R",  # Repository.
        repo_url,
        "-r",  # Root directory.
        sysroot,
        "-U",  # Unpack only.
    ] + pkgs
    env = os.environ.copy()
    env["XBPS_TARGET_ARCH"] = arch
    print(f"Running xbps-install on sysroot {sysroot}")
    if False:
        print(
            "xbps-install command line:",
            " ".join(shlex.quote(s) for s in args),
        )
    process = await asyncio.create_subprocess_exec(
        *args,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    code = await process.wait()
    if code != 0:
        raise RuntimeError(f"xbps-install failed with status {code}")
