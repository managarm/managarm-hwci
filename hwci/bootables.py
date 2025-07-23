import asyncio
import shlex
import sys


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
    print(" ".join(shlex.quote(s) for s in args))
    process = await asyncio.create_subprocess_exec(*args)
    code = await process.wait()
    if code != 0:
        raise RuntimeError(f"gen-tftp.py failed with status {code}")
