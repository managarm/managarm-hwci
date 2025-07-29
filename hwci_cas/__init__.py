import os
import hashlib
import re

from hwci import sqlite_util

SHA256_RE = re.compile(r"^[a-f0-9]{64}$")


class Store:
    __slots__ = ("path", "_db")

    def __init__(self, name, *, dirpath="."):
        self._db = sqlite_util.connect_autocommit(
            os.path.join(dirpath, f"{name}.sqlite")
        )

        # Migrate the DB schema to the newest version.
        with sqlite_util.transaction(self._db):
            (db_version,) = self._db.execute("PRAGMA user_version").fetchone()
            if db_version < 1:
                self._db.execute("""
                    CREATE TABLE objects (
                        hdigest TEXT PRIMARY KEY,
                        meta BLOB,
                        data BLOB
                    )
                    WITHOUT ROWID, STRICT
                """)
                self._db.execute("PRAGMA user_version = 1")

    def write_object(self, hdigest, obj):
        if not SHA256_RE.fullmatch(hdigest):
            raise RuntimeError(f"Rejecting hexdigest: {hdigest}")

        computed_hdigest = obj.hdigest()
        if computed_hdigest != hdigest:
            raise RuntimeError(
                f"SHA256 mismatch, expected {hdigest}, got {computed_hdigest}"
            )

        with sqlite_util.transaction(self._db):
            self._db.execute(
                "INSERT OR IGNORE INTO objects (hdigest, meta, data) VALUES (?, ?, ?)",
                (computed_hdigest, obj.meta, obj.data),
            )

    def read_object(self, hdigest):
        row = self._db.execute(
            "SELECT meta, data FROM objects WHERE hdigest = ?", (hdigest,)
        ).fetchone()
        (meta, data) = row
        return Object(meta, data)

    def extract(self, hdigest, path):
        obj = self.read_object(hdigest)
        with open(path, "wb") as f:
            f.write(obj.data)


class Object:
    ALLOWED_META = {b"b"}

    __slots__ = ("meta", "data")

    @classmethod
    def make_blob(Cls, data):
        return Cls(b"b", data)

    def __init__(self, meta, data):
        if b"\0" in meta:
            raise ValueError(f"Meta value must not contain null bytes: {meta}")
        if meta not in self.ALLOWED_META:
            raise ValueError(f"Meta value not allowed: {meta}")
        self.meta = meta
        self.data = data

    # Computes the hexdigest() that is used to address this object.
    def hdigest(self):
        algo = hashlib.sha256()
        algo.update(self.meta)
        algo.update(b"\0")
        algo.update(self.data)
        return algo.hexdigest()


def serialize(obj):
    return b"\0".join([obj.meta, obj.data])


def deserialize(buf):
    (meta, null, data) = buf.partition(b"\0")
    if not null:
        raise ValueError("CAS object must contain meta data separator")
    return Object(meta, data)
