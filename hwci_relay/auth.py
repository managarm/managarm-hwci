import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os
import secrets
import shlex
import sqlite3
import subprocess
import tempfile
import time
import tomllib

logger = logging.getLogger(__name__)


# check-novalidate does verify that the signature is correct
# but not that the signature comes from a trusted source.
# We use -O print-pubkey to obtain the public key that was used for signing;
# this key can then be validated against a list of trusted keys.
async def ssh_check_novalidate(nonce, signature_path, *, namespace):
    args = [
        "ssh-keygen",
        "-Y",
        "check-novalidate",
        "-q",  # Quiet.
        "-O",
        "print-pubkey",
        "-n",
        namespace,
        "-s",
        signature_path,
    ]
    logger.debug("ssh-keygen command line: %s", " ".join(shlex.quote(s) for s in args))
    process = await asyncio.create_subprocess_exec(
        *args,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )
    (stdout, stderr) = await process.communicate(nonce.encode("ascii"))
    if process.returncode != 0:
        logger.warning("ssh-keygen failed with status %d", process.returncode)
        return None
    return stdout.decode("utf-8").strip()


class Auth:
    HMAC_HASH = "sha256"

    __slots__ = ("_nonce_key", "_nonce_seq", "_auth_db")

    def __init__(self):
        os.makedirs("relay", exist_ok=True)

        self._nonce_key = secrets.token_bytes(
            hashlib.new(self.HMAC_HASH).block_size,
        )
        self._nonce_seq = 1
        self._auth_db = sqlite3.connect("relay/auth.sqlite", autocommit=False)

        # Migrate the DB schema to the newest version.
        with self._auth_db:
            (db_version,) = self._auth_db.execute("PRAGMA user_version").fetchone()
            if db_version < 1:
                self._auth_db.executescript("""
                    CREATE TABLE tokens (
                        identity TEXT,
                        token TEXT PRIMARY KEY,
                        nonce TEXT UNIQUE
                    )
                    WITHOUT ROWID, STRICT;

                    PRAGMA user_version = 1;
                """)

    # Generate a nonce for use in check_nonce():
    # - Nonce validation is stateless.
    # - Each nonce is unique.
    # - Nnonces are valid for 300s.
    # - Nonce are also invalidated by server restart (since _nonce_key is rerolled on restart).
    # Note that the UNIQUE constraint on the tokens table ensures that even with the 300s window,
    # it is not possible to successfully log in twice with the same nonce.
    def nonce(self):
        data = {"s": self._nonce_seq, "t": time.time()}
        self._nonce_seq += 1
        message = json.dumps(data).encode("utf-8")
        mac = hmac.digest(self._nonce_key, message, self.HMAC_HASH)
        parts = [
            base64.urlsafe_b64encode(message).decode("ascii"),
            base64.urlsafe_b64encode(mac).decode("ascii"),
        ]
        return ".".join(parts)

    # Checks the validity of a nonce.
    def check_nonce(self, nonce):
        parts = nonce.split(".")
        message = base64.urlsafe_b64decode(parts[0].encode("ascii"))
        mac = base64.urlsafe_b64decode(parts[1].encode("ascii"))

        # Validate the HMAC.
        computed_mac = hmac.digest(self._nonce_key, message, self.HMAC_HASH)
        if not hmac.compare_digest(mac, computed_mac):
            logger.warning("HMAC mismatch")
            return False

        # Validate that this nonce was generated recently.
        data = json.loads(message)
        now = time.time()
        if data["s"] >= self._nonce_seq:
            raise RuntimeError("Nonce with bad sequence number")
        if now - data["t"] < 0:
            # This can only ever happen if the server clock goes backwards.
            raise RuntimeError("Nonce timestamp in the future")
        if now - data["t"] > 300:
            logger.warning("Nonce check failed due to outdated nonce")
            return False
        return True

    # Authenticates a user identity using an SSH signature.
    # Return either a new access token or None on failure.
    async def authenticate_by_ssh_key(self, nonce, signature):
        # First, check that the nonce is valid.
        if not self.check_nonce(nonce):
            logger.warning("Nonce check failed")
            return None

        # Validate the SSH signature.
        (fd, signature_path) = tempfile.mkstemp(dir=".", prefix="sig-")
        try:
            with open(fd, "wb") as f:
                f.write(signature.encode("ascii"))
            pubkey = await ssh_check_novalidate(
                nonce,
                signature_path,
                namespace="hwci",
            )
        finally:
            os.unlink(signature_path)
        if pubkey is None:
            logger.warning("SSH signature check failed")
            return None
        logger.info("Authentication attempt with SSH key: %s", pubkey)

        # Validate that the SSH key belongs to a known user identity.
        with open("ssh_keys.toml", "rb") as f:
            known_keys = tomllib.load(f)
        identity = known_keys.get(pubkey)
        if identity is None:
            logger.info("SSH key is not in ssh_keys.toml: %s", pubkey)
            return None
        logger.info("Authentication successful for %s", identity)

        # Generate an authentication token that we pass to the client.
        token = secrets.token_urlsafe(32)
        with self._auth_db:
            self._auth_db.execute(
                "INSERT INTO tokens (identity, token, nonce) VALUES (?, ?, ?)",
                (identity, token, nonce),
            )

        return token

    # Checks the validity of an access token.
    def validate_token(self, token):
        with self._auth_db:
            row = self._auth_db.execute(
                "SELECT identity FROM tokens WHERE token = ?",
                (token,),
            ).fetchone()
        if row is None:
            logger.warning("Token not found in auth DB")
            return False
        (identity,) = row
        logger.debug("Token valid for user %s", identity)
        return True
