import asyncio
import os
import shlex
import sys


async def generate_tftp(*, out, profile, sysroot):
    args = [
        sys.executable,
        os.path.join(sysroot, "usr/managarm/bin/gen-boot-artifacts.py"),
        "--sysroot",
        sysroot,
        "--profile",
        profile,
        "-o",
        out,
        "tftp",
    ]
    print(f"Running gen-boot-artifacts.py on sysroot {sysroot}")
    if False:
        print(
            "get-tftp.py command line:",
            " ".join(shlex.quote(s) for s in args),
        )
    process = await asyncio.create_subprocess_exec(*args)
    code = await process.wait()
    if code != 0:
        raise RuntimeError(f"gen-boot-artifacts.py failed with status {code}")
