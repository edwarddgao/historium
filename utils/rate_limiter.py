import asyncio
import time
from typing import Dict

class RateLimiter:
    def __init__(self):
        self._locks: Dict[str, asyncio.Lock] = {}
        self._last_check: Dict[str, float] = {}
        self._rates: Dict[str, tuple] = {}

    def configure(self, name: str, calls: int, period: float):
        """Configure rate limits for a specific museum"""
        self._rates[name] = (calls, period)
        self._locks[name] = asyncio.Lock()
        self._last_check[name] = 0

    async def acquire(self, name: str):
        """Wait until rate limit allows another request"""
        if name not in self._rates:
            return

        calls, period = self._rates[name]
        min_interval = period / calls

        async with self._locks[name]:
            now = time.monotonic()
            time_passed = now - self._last_check[name]
            if time_passed < min_interval:
                await asyncio.sleep(min_interval - time_passed)
            self._last_check[name] = time.monotonic()