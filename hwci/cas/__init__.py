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


def parse_hdigest(hdigest):
    if not SHA256_RE.fullmatch(hdigest):
        raise ValueError(f"Rejecting hexdigest: {hdigest}")
    return bytes.fromhex(hdigest)


class Store:
    __slots__ = ("dirpath", "name", "_db")

    def __init__(self, name, *, dirpath="."):
        self.dirpath = dirpath
        self.name = name

        self._init_db()
        self._attach_data_db()
        self._init_db_views()

    def _init_db(self):
        self._db = sqlite_util.connect_autocommit(
            os.path.join(self.dirpath, f"{self.name}.sqlite")
        )

        # Set per-connection pragmas.
        self._db.execute("PRAGMA main.locking_mode = EXCLUSIVE")
        self._db.execute("PRAGMA main.mmap_size = 0x80000000")

        # Set per-DB pragmas.
        self._db.execute("PRAGMA main.journal_mode = WAL")
        self._db.execute("PRAGMA main.synchronous = NORMAL")

        # Migrate the DB schema to the newest version.
        # Brackets indicate the affected tables.
        # History:
        # v1 [objects]: Used a WITHOUT ROWID table which lead to poor performance
        #     as it stored the (relatively large) blobs inside the B-tree.
        # v2 [objects]: Remove WITHOUT ROWID.
        # v3 [objects]: Add last_use column.
        # v4 [objects]: Add compression column.
        # v5 [objects]: Replace hex digests by binary digests.
        # v6 [objects]: Split data into attached database.
        # v7 [digests, objects]: Split digests into own table.
        # v8 [links]: Add table for links [main.links].
        with sqlite_util.transaction(self._db):
            (db_version,) = self._db.execute("PRAGMA main.user_version").fetchone()
            if db_version < 8:
                self._db.execute("DROP TABLE IF EXISTS main.objects")
                # We store digests in a separate table.
                # This has the advantage that we can first allocate IDs, then commit the
                # object data and finally insert object meta data (without doing UPDATEs).
                # Since SQLite stores UNIQUE indices as B-trees that map keys to row IDs, it does
                # not make a difference for lookup performance whether a separate table is used
                # or not (however, it does require SQLite to do two lookups when querying
                # both fields from objects and the digest by ID).
                self._db.execute("""
                    CREATE TABLE main.digests (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        digest BLOB UNIQUE
                    )
                    STRICT
                """)
                self._db.execute("""
                    CREATE TABLE main.objects (
                        id INTEGER PRIMARY KEY,
                        meta BLOB,
                        last_use INTEGER
                    )
                    STRICT
                """)
                self._db.execute("""
                    CREATE TABLE main.links (
                        id INTEGER,
                        rank INTEGER,
                        target_digest BLOB,
                        PRIMARY KEY (id, rank)
                    )
                    STRICT
                """)
                self._db.execute("PRAGMA main.user_version = 8")

    def _attach_data_db(self):
        self._db.execute(
            "ATTACH DATABASE ? AS data",
            (os.path.join(self.dirpath, f"{self.name}.data-sqlite"),),
        )

        # Set per-connection pragmas.
        self._db.execute("PRAGMA data.locking_mode = EXCLUSIVE")

        # Set per-DB pragmas.
        self._db.execute("PRAGMA data.journal_mode = WAL")
        self._db.execute("PRAGMA data.synchronous = NORMAL")

        # Migrate the DB schema to the newest version.
        # Brackets indicate the affected tables.
        # History:
        # v1 [buffers]: Split data into attached database.
        with sqlite_util.transaction(self._db):
            (db_version,) = self._db.execute("PRAGMA data.user_version").fetchone()
            if db_version < 1:
                # We split the buffers into a separate file to keep the main DB file small.
                # This ensures that the entire main DB can be mmap()ed at the same time.
                self._db.execute("""
                    CREATE TABLE data.buffers (
                        id INTEGER PRIMARY KEY,
                        compression BLOB,
                        buffer BLOB
                    )
                    STRICT
                """)
                self._db.execute("PRAGMA data.user_version = 1")

    def _init_db_views(self):
        # We create our views in the temporary in-memory DB.
        # Storing them to disk does not offer any advantages anyway.
        self._db.execute("""
            CREATE TEMP VIEW objects_full
            AS SELECT id, digest, meta, last_use
            FROM digests
            NATURAL INNER JOIN objects
        """)
        self._db.execute("""
            CREATE TEMP VIEW objects_with_buffers
            AS SELECT id, digest, meta, last_use, compression, buffer FROM digests
            NATURAL INNER JOIN objects
            NATURAL INNER JOIN buffers
        """)
        # Doing the LEFT_JOINs on both tables makes sqlite use a smarter query plan for the walk query.
        # If we instead JOIN with objects_full, it tries to materialize objects_full.
        self._db.execute("""
            CREATE TEMP VIEW links_with_target_objects
            AS SELECT links.id, rank, target_digest, digests.id AS target_id, objects.meta AS target_meta
            FROM links
            LEFT JOIN digests ON links.target_digest = digests.digest
            LEFT JOIN objects ON digests.id = objects.id;
        """)

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
        timestamp = int(time.time())

        new_objects = []
        new_links = []
        for hdigest, objbuf in iterable:
            digest = parse_hdigest(hdigest)
            new_objects.append((digest, objbuf))

            if objbuf.meta == b"m":
                obj = objbuf.to_object()
                new_links.append((digest, list(obj.merkle_link_digests())))

        with (
            hwci.timer_util.Timer() as commit_timer,
            sqlite_util.transaction(self._db),
        ):
            self._db.executemany(
                "INSERT INTO digests (digest) VALUES (?) ON CONFLICT DO NOTHING",
                ((digest,) for digest, objbuf in new_objects),
            )

            self._db.executemany(
                "INSERT INTO objects (id, meta, last_use)"
                " SELECT (SELECT id FROM digests WHERE digest = ?), ?, ?"
                " ON CONFLICT DO NOTHING",
                ((digest, objbuf.meta, timestamp) for digest, objbuf in new_objects),
            )

            self._db.executemany(
                "INSERT INTO buffers (id, compression, buffer)"
                " SELECT (SELECT id FROM digests WHERE digest = ?), ?, ?"
                " ON CONFLICT DO NOTHING",
                (
                    (digest, objbuf.compression, objbuf.buffer)
                    for digest, objbuf in new_objects
                ),
            )

            for digest, link_digests in new_links:
                row = self._db.execute(
                    "SELECT id FROM digests WHERE digest = ?", (digest,)
                ).fetchone()
                assert row is not None
                (obj_id,) = row

                self._db.executemany(
                    "INSERT INTO links (id, rank, target_digest) VALUES (?, ?, ?)"
                    " ON CONFLICT DO NOTHING",
                    (
                        (obj_id, i, link_digest)
                        for i, link_digest in enumerate(link_digests)
                    ),
                )

        logger.debug(
            "Wrote %d objects, %d links; transaction took %.2f s",
            len(new_objects),
            sum(len(link_digests) for digest, link_digests in new_links),
            commit_timer.elapsed,
        )

    def read_meta_or_none(self, hdigest):
        row = self._db.execute(
            "SELECT meta FROM objects_full WHERE digest = ?", (parse_hdigest(hdigest),)
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
            "SELECT meta, compression, buffer FROM objects_with_buffers WHERE digest = ?",
            (parse_hdigest(hdigest),),
        ).fetchone()
        if row is None:
            return None
        (meta, compression, buffer) = row
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

    def walk_tree_hdigests_into(self, root_hdigest, *, hdigest_set, missing_set):
        # Stores (obj_id, meta) pairs.
        q = []

        def visit(new_hdigest, new_obj_id, new_meta):
            if new_meta is None:
                missing_set.add(new_hdigest)
            elif new_hdigest not in hdigest_set:
                hdigest_set.add(new_hdigest)
                if new_meta == b"m":
                    q.append((new_obj_id, new_meta))

        root_row = self._db.execute(
            "SELECT id, meta FROM objects_full WHERE digest = ?",
            (parse_hdigest(root_hdigest),),
        ).fetchone() or (None, None)
        root_obj_id, root_meta = root_row
        visit(root_hdigest, root_obj_id, root_meta)

        while q:
            obj_id, meta = q.pop()
            assert meta == b"m"

            rows = self._db.execute(
                "SELECT target_digest, target_id, target_meta FROM links_with_target_objects WHERE id = ?",
                (obj_id,),
            ).fetchall()
            for row in rows:
                link_digest, link_obj_id, link_meta = row
                visit(link_digest.hex(), link_obj_id, link_meta)

    def bump(self, hdigests):
        timestamp = int(time.time())
        with sqlite_util.transaction(self._db):
            self._db.executemany(
                "UPDATE objects SET last_use = ?"
                " FROM digests WHERE objects.id = digests.id AND digest = ?",
                [(timestamp, parse_hdigest(hdigest)) for hdigest in hdigests],
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
                "SELECT SUM(LENGTH(buffer)) FROM buffers"
            ).fetchone()
            if total_size is None:
                total_size = 0
            logger.info("Total size is %d", total_size)

            size_threshold = 10 * 1024 * 1024 * 1024  # 10 GiB.

            obj_ids_to_delete = []
            cursor = self._db.execute(
                "SELECT id, digest, last_use, LENGTH(buffer) FROM objects_with_buffers ORDER BY last_use ASC"
            )
            cutoff = int(time.time()) - 7 * 24 * 60 * 60  # 7 days.
            for obj_id, digest, last_use, size in cursor:
                if total_size < size_threshold and last_use >= cutoff:
                    break
                if digest.hex() not in full_keep_set:
                    obj_ids_to_delete.append((obj_id,))
                    total_size -= size

            logger.info("Deleting %d objects", len(obj_ids_to_delete))
            self._db.executemany("DELETE FROM buffers WHERE id = ?", obj_ids_to_delete)
            self._db.executemany("DELETE FROM objects WHERE id = ?", obj_ids_to_delete)
            self._db.executemany("DELETE FROM links WHERE id = ?", obj_ids_to_delete)
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

    def validate(self, hdigest):
        if not SHA256_RE.fullmatch(hdigest):
            raise RuntimeError(f"Rejecting hexdigest: {hdigest}")

        computed_hdigest = self.to_object().hdigest()
        if computed_hdigest != hdigest:
            raise RuntimeError(
                f"SHA256 mismatch, expected {hdigest}, got {computed_hdigest}"
            )


class Serializer:
    __slots__ = ("buf",)

    def __init__(self):
        self.buf = bytearray()

    def serialize(self, digest, objbuf):
        assert len(digest) == DIGEST_SIZE

        total = (
            4  # Total size.
            + DIGEST_SIZE
            + len(objbuf.meta)
            + 1
            + len(objbuf.compression)
            + 1
            + len(objbuf.buffer)
        )
        p = len(self.buf)
        self.buf += total.to_bytes(4, "little")
        self.buf += digest
        self.buf += objbuf.meta
        self.buf += b"\0"
        self.buf += objbuf.compression
        self.buf += b"\0"
        self.buf += objbuf.buffer
        assert len(self.buf) == p + total


class Deserializer:
    __slots__ = ("buf", "pos")

    def __init__(self, buf):
        self.buf = buf
        self.pos = 0

    def deserialize(self):
        p = self.pos
        if p == len(self.buf):
            return None
        assert p < len(self.buf)

        # Total.
        total = int.from_bytes(self.buf[p : p + 4], "little")
        limit = self.pos + total
        p += 4
        # Digest.
        digest = self.buf[p : p + DIGEST_SIZE]
        p += DIGEST_SIZE
        # Meta.
        s = self.buf.index(b"\0", p, limit)
        meta = self.buf[p:s]
        p = s + 1
        # Compression.
        s = self.buf.index(b"\0", p, limit)
        compression = self.buf[p:s]
        p = s + 1
        # Buffer.
        buffer = self.buf[p:limit]

        self.pos += total
        return (digest, ObjectBuffer(meta, buffer, compression=compression))


def serialize(digest, objbuf):
    ser = Serializer()
    ser.serialize(digest, objbuf)
    return ser.buf


def deserialize(buf):
    der = Deserializer(buf)
    item = der.deserialize()
    if der.pos != len(der.buf):
        raise ValueError("Buffer contains more than one object")
    return item


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
