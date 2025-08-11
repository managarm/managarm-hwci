import asyncio
import concurrent.futures
import hashlib
import logging
import math
import mmap
import os
import re
import time
import zstandard

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

        # Set per-connection pragmas.
        self._db.execute("PRAGMA locking_mode = EXCLUSIVE")

        # Set per-DB pragmas.
        self._db.execute("PRAGMA journal_mode = WAL")
        self._db.execute("PRAGMA synchronous = NORMAL")

        # Migrate the DB schema to the newest version.
        # History:
        # v1: Used a WITHOUT ROWID table which lead to poor performance
        #     as it stored the (relatively large) blobs inside the B-tree.
        # v2: Remove WITHOUT ROWID.
        # v3: Add last_use column.
        # v4: Add compression column.
        with sqlite_util.transaction(self._db):
            (db_version,) = self._db.execute("PRAGMA user_version").fetchone()
            if db_version < 4:
                self._db.execute("DROP TABLE IF EXISTS objects")
                self._db.execute("""
                    CREATE TABLE objects (
                        hdigest TEXT PRIMARY KEY,
                        meta BLOB,
                        data BLOB,
                        compression BLOB,
                        last_use INTEGER
                    )
                    STRICT
                """)
                self._db.execute("PRAGMA user_version = 4")

    def write_object(self, hdigest, obj):
        singleton_list = [(hdigest, obj)]
        self.write_many_objects(singleton_list)

    def write_many_objects(self, iterable):
        self.write_many_object_buffers(
            (hdigest, obj.to_object_buffer()) for hdigest, obj in iterable
        )

    def write_object_buffer(self, hdigest, objbuf):
        singleton_list = [(hdigest, objbuf)]
        self.write_many_object_buffers(singleton_list)

    def write_many_object_buffers(self, iterable):
        hash_timer = hwci.timer_util.Timer()
        inserts = []
        timestamp = int(time.time())
        for hdigest, objbuf in iterable:
            if not SHA256_RE.fullmatch(hdigest):
                raise RuntimeError(f"Rejecting hexdigest: {hdigest}")

            with hash_timer:
                computed_hdigest = objbuf.to_object().hdigest()
            if computed_hdigest != hdigest:
                raise RuntimeError(
                    f"SHA256 mismatch, expected {hdigest}, got {computed_hdigest}"
                )

            inserts.append(
                (
                    computed_hdigest,
                    objbuf.meta,
                    objbuf.buffer,
                    objbuf.compression,
                    timestamp,
                )
            )

        with (
            hwci.timer_util.Timer() as commit_timer,
            sqlite_util.transaction(self._db),
        ):
            self._db.executemany(
                "INSERT OR IGNORE INTO objects (hdigest, meta, data, compression, last_use)"
                " VALUES (?, ?, ?, ?, ?)",
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

    def read_object_buffer_or_none(self, hdigest):
        row = self._db.execute(
            "SELECT meta, data, compression FROM objects WHERE hdigest = ?", (hdigest,)
        ).fetchone()
        if row is None:
            return None
        (meta, buffer, compression) = row
        return ObjectBuffer(meta, buffer, compression=compression)

    def read_object_buffer(self, hdigest):
        objbuf = self.read_object_buffer_or_none(hdigest)
        if objbuf is None:
            raise KeyError(f"Missing object: {hdigest}")
        return objbuf

    def read_object_or_none(self, hdigest):
        return self.read_object_buffer_or_none(hdigest).to_object()

    def read_object(self, hdigest):
        return self.read_object_buffer(hdigest).to_object()

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

    def bump(self, hdigests):
        timestamp = int(time.time())
        with sqlite_util.transaction(self._db):
            self._db.executemany(
                "UPDATE objects SET last_use = ? WHERE hdigest = ?",
                [(timestamp, hdigest) for hdigest in hdigests],
            )

    def prune(self, keep_hdigests):
        logger.info("Running object storage pruning")

        with hwci.timer_util.Timer() as walk_timer:
            # We can ignore missing_set here.
            full_keep_set = set()
            missing_set = set()
            for hdigest in keep_hdigests:
                self.walk_tree_hdigests_into(
                    hdigest, hdigest_set=full_keep_set, missing_set=missing_set
                )
            logger.info(
                "Keeping %d objects (walked trees in %.2f s)",
                len(full_keep_set),
                walk_timer.elapsed,
            )

        with (
            hwci.timer_util.Timer() as tx_timer,
            sqlite_util.transaction(self._db),
        ):
            (total_size,) = self._db.execute(
                "SELECT SUM(LENGTH(data)) FROM objects"
            ).fetchone()
            if total_size is None:
                total_size = 0
            logger.info("Total size is %d", total_size)

            size_threshold = 10 * 1024 * 1024 * 1024  # 10 GiB.

            hdigests_to_delete = []
            cursor = self._db.execute(
                "SELECT hdigest, last_use, LENGTH(data) FROM objects ORDER BY last_use ASC"
            )
            cutoff = int(time.time()) - 7 * 24 * 60 * 60  # 7 days.
            for hdigest, last_use, size in cursor:
                if total_size < size_threshold and last_use >= cutoff:
                    break
                if hdigest not in full_keep_set:
                    hdigests_to_delete.append((hdigest,))
                    total_size -= size

            logger.info("Deleting %d objects", len(hdigests_to_delete))
            self._db.executemany(
                "DELETE FROM objects WHERE hdigest = ?", hdigests_to_delete
            )
        logger.debug("Pruning transaction took %.2f s", tx_timer.elapsed)

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

    def to_object_buffer(self):
        return ObjectBuffer(self.meta, self.data, compression=b"")

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


# Represents a potentially compressed buffer that can be decompressed to an Object.
class ObjectBuffer:
    ALLOWED_COMPRESSION = {b"", b"z"}

    __slots__ = ("meta", "buffer", "compression")

    def __init__(self, meta, buffer, *, compression):
        if b"\0" in meta:
            raise ValueError(f"Meta value must not contain null bytes: {meta}")
        if meta not in Object.ALLOWED_META:
            raise ValueError(f"Meta value not allowed: {meta}")
        if compression not in self.ALLOWED_COMPRESSION:
            raise ValueError(f"Compression not supported: {compression}")
        self.meta = meta
        self.buffer = buffer
        self.compression = compression

    def to_object(self):
        objbuf = self.to_decompressed()
        assert objbuf.compression == b""
        return Object(objbuf.meta, objbuf.buffer)

    def to_compressed(self, *, compressor=None):
        if self.compression == b"z":
            return self
        else:
            assert not self.compression
            if not compressor:
                compressor = zstandard.ZstdCompressor()
            return ObjectBuffer(
                self.meta, compressor.compress(self.buffer), compression=b"z"
            )

    def to_decompressed(self, *, decompressor=None):
        if self.compression == b"z":
            if not decompressor:
                decompressor = zstandard.ZstdDecompressor()
            return ObjectBuffer(
                self.meta, decompressor.decompress(self.buffer), compression=b""
            )
        else:
            assert not self.compression
            return self


class Serializer:
    __slots__ = ("buf",)

    def __init__(self):
        self.buf = bytearray()

    def serialize(self, objbuf):
        total = (
            4  # Total size.
            + 16  # Digest. TODO: We currently do not save the digest.
            + len(objbuf.meta)
            + 1
            + len(objbuf.compression)
            + 1
            + len(objbuf.buffer)
        )
        self.buf += total.to_bytes(4, "little")
        self.buf += bytes(16)
        self.buf += objbuf.meta
        self.buf += b"\0"
        self.buf += objbuf.compression
        self.buf += b"\0"
        self.buf += objbuf.buffer


class Deserializer:
    __slots__ = ("buf",)

    def __init__(self, buf):
        self.buf = buf

    def deserialize(self):
        p = 0
        # Total.
        total = int.from_bytes(self.buf[p : p + 4], "little")
        assert total == len(self.buf)
        p += 4
        # Digest.
        self.buf[p : p + 16]
        p += 16
        # Meta.
        s = self.buf.index(b"\0", p)
        meta = self.buf[p:s]
        p = s + 1
        # Compression.
        s = self.buf.index(b"\0", p)
        compression = self.buf[p:s]
        p = s + 1
        # Buffer.
        buffer = self.buf[p:total]

        return ObjectBuffer(meta, buffer, compression=compression)


def serialize(objbuf):
    ser = Serializer()
    ser.serialize(objbuf)
    return ser.buf


def deserialize(buf):
    der = Deserializer(buf)
    return der.deserialize()


class Dissector:
    # Chunk size.
    CS = 16 * 1024
    # Work size: number of chunks hashed by each parallel job.
    WS = 4096

    __slots__ = ("window", "_view", "_refs")

    def __init__(self, f):
        self.window = mmap.mmap(f.fileno(), 0, prot=mmap.PROT_READ, trackfd=False)
        self._view = memoryview(self.window)
        self._refs = []

    def close(self):
        self._view.release()
        for obj in self._refs:
            obj.data.release()
        self.window.close()

    async def dissect_into(self, *, object_dict):
        out = []
        num_jobs = math.ceil(len(self._view) / (self.WS * self.CS))
        if num_jobs > 1:
            # Parallel implementation.
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=os.process_cpu_count()
            ) as executor:
                futures = []
                loop = asyncio.get_running_loop()
                # TODO: Instead of submitting all jobs at the same time,
                #       only submit a fixed number at a time.
                for offset in range(0, len(self._view), self.WS * self.CS):
                    futures.append(
                        loop.run_in_executor(executor, self._dissect_chunks, offset)
                    )
                for fut in futures:
                    out.extend(await fut)
        else:
            for offset in range(0, len(self._view), self.WS * self.CS):
                out.extend(self._dissect_chunks(offset))

        for digest, obj in out:
            object_dict.setdefault(digest.hex(), obj)
            self._refs.append(obj)

        root = Object.make_merkle(b"".join(digest for digest, obj in out))
        object_dict.setdefault(root.hdigest(), root)

        return root.hdigest()

    # This function hashes WS chunks of the input starting at some offset.
    # We hash more than one chunk at a time to improve multi-threaded performance.
    def _dissect_chunks(self, offset):
        n = len(self._view)
        out = []
        for c in range(offset, min(offset + self.WS * self.CS, n), self.CS):
            chunk = self._view[c : min(c + self.CS, n)]
            obj = Object.make_blob(chunk)
            out.append((obj.digest(), obj))
        return out
