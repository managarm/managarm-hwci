import asyncio
import logging
import os
import shlex
import subprocess

logger = logging.getLogger(__name__)


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
    logger.info("Running xbps-install on sysroot %s", sysroot)
    logger.debug(
        "xbps-install command line: %s",
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
