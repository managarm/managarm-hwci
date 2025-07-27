import asyncio
import logging
import shlex
import sys

logger = logging.getLogger(__name__)


async def generate_tftp(*, out, profile, sysroot):
    args = [
        sys.executable,
        "gen-tftp.py",
        "--sysroot",
        sysroot,
        "--profile",
        profile,
        out,
    ]
    logger.info("Running gen-tftp.py on sysroot %s", sysroot)
    logger.debug(
        "get-tftp.py command line: %s",
        " ".join(shlex.quote(s) for s in args),
    )
    process = await asyncio.create_subprocess_exec(*args)
    code = await process.wait()
    if code != 0:
        raise RuntimeError(f"gen-tftp.py failed with status {code}")
