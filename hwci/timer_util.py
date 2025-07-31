import time


class Timer:
    __slots__ = ("elapsed", "_s")

    def __init__(self):
        self.elapsed = 0
        self._s = None

    def __enter__(self):
        assert self._s is None
        self._s = time.monotonic()
        return self

    def __exit__(self, exc_type, exc_value, tb):
        assert self._s is not None
        self.elapsed += time.monotonic() - self._s
        self._s = None
