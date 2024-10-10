import asyncio
from collections import deque


class Stats:
    def __init__(self):
        self._is_success = deque(maxlen=100)
        self._success_cost = deque(maxlen=100)
        self._fail_cost = deque(maxlen=100)
        self._lock = asyncio.Lock()


    async def update(self, is_success, cost):
        async with self._lock:
            self._is_success.append(is_success)
            if is_success:
                self._success_cost.append(cost)
            else:
                self._fail_cost.append(cost)


    async def total_cost(self, fallback_cost):
        success_prob = await self.success_prob()
        success_cost = await self.success_cost()
        fail_cost = await self.fail_cost()
        return success_prob * success_cost + ((1 - success_prob) * (fail_cost + fallback_cost))


    async def success_prob(self):
        async with self._lock:
            if len(self._is_success) == 0:
                return 0.0
            return self._is_success.count(True) / len(self._is_success)


    async def success_cost(self):
        async with self._lock:
            if len(self._success_cost) == 0:
                return 0.0
            return sum(self._success_cost) / len(self._success_cost)


    async def fail_cost(self):
        async with self._lock:
            if len(self._fail_cost) == 0:
                return 0.0
            return sum(self._fail_cost) / len(self._fail_cost)
