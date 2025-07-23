import urllib.parse


async def json_rpc(session, base_url, *, method, params):
    json_req = {
        "id": 1,
        "src": "managarm-hwci",
        "method": method,
        "params": params,
    }
    url = urllib.parse.urljoin(base_url, "rpc")
    async with session.post(url, json=json_req) as response:
        response.raise_for_status()  # Raise for HTTP error codes.
        json_resp = await response.json()
        if json_resp["id"] != 1:
            raise RuntimeError(
                "JSON RPC id mismatch: expected {}, received {}".format(
                    1, json_resp["id"]
                )
            )
        if "error" in json_resp:
            raise RuntimeError("JSON RPC error: {}".format(json_resp["error"]))
        if "result" not in json_resp:
            raise RuntimeError("JSON RPC returned neither error nor result")
        return json_resp["result"]


async def shelly_switch_get_status(session, base_url):
    return await json_rpc(
        session,
        base_url,
        method="Switch.GetStatus",
        params={"id": 0},
    )


async def shelly_switch_set(session, base_url, *, on):
    return await json_rpc(
        session,
        base_url,
        method="Switch.Set",
        params={"id": 0, "on": on},
    )


class Switch:
    __slots__ = (
        "session",
        "base_url",
    )

    def __init__(self, session, base_url):
        self.session = session
        self.base_url = base_url

    async def ensure_off(self):
        status = await shelly_switch_get_status(self.session, self.base_url)
        if status["output"]:
            raise RuntimeError("Expected power to be OFF but it is ON")

    async def flip_on(self):
        await shelly_switch_set(self.session, self.base_url, on=True)

    async def flip_off(self):
        await shelly_switch_set(self.session, self.base_url, on=False)
