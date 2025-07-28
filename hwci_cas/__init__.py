import os
import hashlib
import tempfile
import re
import shutil

SHA256_RE = re.compile(r"^[a-f0-9]{64}$")


class Store:
    __slots__ = ("path",)

    def __init__(self, path):
        self.path = path

    def write_object(self, hdigest, data):
        if not SHA256_RE.fullmatch(hdigest):
            raise RuntimeError(f"Rejecting hexdigest: {hdigest}")

        computed_hdigest = hashlib.sha256(data).hexdigest()
        if computed_hdigest != hdigest:
            raise RuntimeError(
                f"SHA256 mismatch, expected {hdigest}, got {computed_hdigest}"
            )

        committed = False
        os.makedirs(self.path, exist_ok=True)
        (fd, temp_path) = tempfile.mkstemp(dir=self.path, prefix="temp-")
        try:
            with open(fd, "wb") as f:
                f.write(data)
            os.rename(temp_path, os.path.join(self.path, computed_hdigest))
            committed = True
        finally:
            if not committed:
                os.unlink(temp_path)

    def read_object(self, hdigest):
        with open(os.path.join(self.path, hdigest), "rb") as f:
            return f.read()

    def extract(self, hdigest, path):
        shutil.copyfile(
            os.path.join(self.path, hdigest),
            path,
        )
