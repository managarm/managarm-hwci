import urllib.parse
import subprocess


async def nanokvm_login(session, base_url, *, username, password):
    json_req = {
        "username": username,
        "password": password,
    }
    url = urllib.parse.urljoin(base_url, "api/auth/login")
    async with session.post(url, json=json_req) as response:
        response.raise_for_status()  # Raise for HTTP error codes.
        json_resp = await response.json()
        return json_resp["data"]


async def nanokvm_get_gpio(session, base_url, token):
    url = urllib.parse.urljoin(base_url, "api/vm/gpio")
    async with session.get(
        url, headers={"Cookie": f"nano-kvm-token={token}"}
    ) as response:
        response.raise_for_status()  # Raise for HTTP error codes.
        json_resp = await response.json()
        return json_resp["data"]


async def nanokvm_post_gpio(session, base_url, token, *, what, duration):
    json_req = {
        "type": what,
        "duration": duration,
    }
    url = urllib.parse.urljoin(base_url, "api/vm/gpio")
    async with session.post(
        url, headers={"Cookie": f"nano-kvm-token={token}"}, json=json_req
    ) as response:
        response.raise_for_status()  # Raise for HTTP error codes.
        json_resp = await response.json()
        return json_resp["data"]


class Switch:
    __slots__ = (
        "session",
        "base_url",
        "username",
        "password",
        "_token",
    )

    def __init__(self, session, base_url, *, username, password):
        self.session = session
        self.base_url = base_url
        self.username = username
        self.password = password
        self._token = None

    async def ensure_off(self):
        if not self._token:
            await self._login()
        status = await nanokvm_get_gpio(self.session, self.base_url, self._token)
        if status["pwr"]:
            raise RuntimeError("Expected power to be OFF but it is ON")

    async def flip_on(self):
        if not self._token:
            await self._login()
        await nanokvm_post_gpio(
            self.session, self.base_url, self._token, what="power", duration=800
        )

    async def flip_off(self):
        if not self._token:
            await self._login()
        await nanokvm_post_gpio(
            self.session, self.base_url, self._token, what="power", duration=800
        )

    async def _login(self):
        # NanoKvm encrypts the password with a statically known key...
        completed = subprocess.run(
            [
                "openssl",
                "enc",
                "-aes-256-cbc",
                "-a",  # base64 encode.
                "-A",  # base64 without newlines.
                "-salt",
                "-md",
                "md5",
                "-pass",
                "pass:nanokvm-sipeed-2024",
            ],
            input=self.password,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
        )
        result = await nanokvm_login(
            self.session,
            self.base_url,
            username=self.username,
            password=urllib.parse.quote(completed.stdout),
        )
        self._token = result["token"]
