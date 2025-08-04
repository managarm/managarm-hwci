import asyncio
import concurrent.futures
import hashlib
import logging
import mmap
import os
import re

from hwci import sqlite_util
import hwci.timer_util

logger = logging.getLogger(__name__)

SHA256_RE = re.compile(r"^[a-f0-9]{64}$")

DIGEST_SIZE = hashlib.sha256().digest_size


class Store:
    __slots__ = ("path", "_db")

    def __init__(self, name, *, dirpath="."):
        self._db = sqlite_util.connect_autocommit(
            os.path.join(dirpath, f"{name}.sqlite")
        )

        # Migrate the DB schema to the newest version.
        # History:
        # v1: Used a WITHOUT ROWID table which lead to poor performance
        #     as it stored the (relatively large) blobs inside the B-tree.
        with sqlite_util.transaction(self._db):
            (db_version,) = self._db.execute("PRAGMA user_version").fetchone()
            if db_version < 2:
                self._db.execute("DROP TABLE IF EXISTS objects")
                self._db.execute("""
                    CREATE TABLE objects (
                        hdigest TEXT PRIMARY KEY,
                        meta BLOB,
                        data BLOB
                    )
                    STRICT
                """)
                self._db.execute("PRAGMA user_version = 2")

    def write_object(self, hdigest, obj):
        singleton_list = [(hdigest, obj)]
        self.write_many_objects(singleton_list)

    def write_many_objects(self, iterable):
        hash_timer = hwci.timer_util.Timer()
        inserts = []
        for hdigest, obj in iterable:
            if not SHA256_RE.fullmatch(hdigest):
                raise RuntimeError(f"Rejecting hexdigest: {hdigest}")

            with hash_timer:
                computed_hdigest = obj.hdigest()
            if computed_hdigest != hdigest:
                raise RuntimeError(
                    f"SHA256 mismatch, expected {hdigest}, got {computed_hdigest}"
                )

            inserts.append((computed_hdigest, obj.meta, obj.data))

        with (
            hwci.timer_util.Timer() as commit_timer,
            sqlite_util.transaction(self._db),
        ):
            self._db.executemany(
                "INSERT OR IGNORE INTO objects (hdigest, meta, data) VALUES (?, ?, ?)",
                inserts,
            )
        logger.debug(
            "Wrote %d objects (committed in %.2f s, hashing took %.2f s)",
            len(inserts),
            hash_timer.elapsed,
            commit_timer.elapsed,
        )

    def read_meta_or_none(self, hdigest):
        row = self._db.execute(
            "SELECT meta FROM objects WHERE hdigest = ?", (hdigest,)
        ).fetchone()
        if row is None:
            return None
        (meta,) = row
        return meta

    def read_meta(self, hdigest):
        meta = self.read_meta_or_none(hdigest)
        if meta is None:
            raise KeyError(f"Missing object: {hdigest}")
        return meta

    def read_object_or_none(self, hdigest):
        row = self._db.execute(
            "SELECT meta, data FROM objects WHERE hdigest = ?", (hdigest,)
        ).fetchone()
        if row is None:
            return None
        (meta, data) = row
        return Object(meta, data)

    def read_object(self, hdigest):
        obj = self.read_object_or_none(hdigest)
        if obj is None:
            raise KeyError(f"Missing object: {hdigest}")
        return obj

    def walk_tree_hdigests_into(self, hdigest, *, hdigest_set, missing_set=None):
        q = [hdigest]
        while q:
            hdigest = q.pop()
            if hdigest in hdigest_set:
                continue
            meta = self.read_meta_or_none(hdigest)
            if meta is None:
                if missing_set is None:
                    raise KeyError(f"Missing object: {hdigest}")
                missing_set.add(hdigest)
                continue
            hdigest_set.add(hdigest)
            if meta in {b"m"}:
                obj = self.read_object(hdigest)
                q.extend(obj.iter_linked())
            else:
                assert meta == b"b"

    def extract(self, hdigest, path):
        with open(path, "wb") as f:
            self.extract_to(hdigest, f)

    def extract_to(self, hdigest, f):
        obj = self.read_object(hdigest)
        if obj.meta == b"m":
            for link_hdigest in obj.merkle_link_hdigests():
                self.extract_to(link_hdigest, f)
        else:
            assert obj.meta == b"b"
            f.write(obj.data)


class Object:
    ALLOWED_META = {b"b", b"m"}

    __slots__ = ("meta", "data")

    @classmethod
    def make_blob(Cls, data):
        return Cls(b"b", data)

    @classmethod
    def make_merkle(Cls, data):
        return Cls(b"m", data)

    def __init__(self, meta, data):
        if b"\0" in meta:
            raise ValueError(f"Meta value must not contain null bytes: {meta}")
        if meta not in self.ALLOWED_META:
            raise ValueError(f"Meta value not allowed: {meta}")
        self.meta = meta
        self.data = data

    def digest(self):
        algo = hashlib.sha256()
        algo.update(self.meta)
        algo.update(b"\0")
        algo.update(self.data)
        return algo.digest()

    # Computes the hexdigest() that is used to address this object.
    def hdigest(self):
        return self.digest().hex()

    def merkle_link_digests(self):
        if self.meta != b"m":
            raise ValueError("Object is not a Merkle object")
        return [
            self.data[d : d + DIGEST_SIZE]
            for d in range(0, len(self.data), DIGEST_SIZE)
        ]

    def merkle_link_hdigests(self):
        return [digest.hex() for digest in self.merkle_link_digests()]

    def iter_linked(self):
        if self.meta == b"m":
            yield from self.merkle_link_hdigests()
        else:
            assert self.meta == b"b"


def serialize(obj):
    return b"\0".join([obj.meta, obj.data])


def deserialize(buf):
    (meta, null, data) = buf.partition(b"\0")
    if not null:
        raise ValueError("CAS object must contain meta data separator")
    return Object(meta, data)


class Dissector:
    # Chunk size.
    CS = 16 * 1024
    # Work size: number of chunks hashed by each parallel job.
    WS = 4096

    __slots__ = ("f",)

    def __init__(self, f):
        self.f = f

    async def dissect_into(self, *, object_dict):
        out = []
        with mmap.mmap(
            self.f.fileno(), 0, prot=mmap.PROT_READ, trackfd=False
        ) as window:
            if False:
                # Parallel implementation. Disabled for now since it's only worthwhile for GiB-sized files.
                with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
                    futures = []
                    loop = asyncio.get_running_loop()
                    # TODO: Instead of submitting all jobs at the same time,
                    #       only submit a fixed number at a time.
                    for offset in range(0, len(window), self.WS * self.CS):
                        futures.append(
                            loop.run_in_executor(
                                executor, self._dissect_chunks, window, offset
                            )
                        )
                    for fut in futures:
                        out.extend(await fut)
            else:
                for offset in range(0, len(window), self.WS * self.CS):
                    out.extend(self._dissect_chunks(window, offset))

        for digest, obj in out:
            object_dict.setdefault(digest.hex(), obj)

        root = Object.make_merkle(b"".join(digest for digest, obj in out))
        object_dict.setdefault(root.hdigest(), root)

        return root.hdigest()

    # This function hashes WS chunks of the input starting at some offset.
    # We hash more than one chunk at a time to improve multi-threaded performance.
    # window is bytes-like buffer (e.g., returned by mmap.mmap()).
    def _dissect_chunks(self, window, offset):
        n = len(window)
        out = []
        for c in range(offset, min(offset + self.WS * self.CS, n), self.CS):
            chunk = window[c : min(c + self.CS, n)]
            obj = Object.make_blob(chunk)
            out.append((obj.digest(), obj))
        return out
