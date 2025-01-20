import asyncio
from collections import deque


class Stats:
    '''Information about costs and success probability of algorithm.'''
    def __init__(self):
        '''Constructor.'''
        self._is_success = deque(maxlen=100)
        self._success_cost = deque(maxlen=100)
        self._fail_cost = deque(maxlen=100)
        self._lock = asyncio.Lock()


    async def update(self, is_success, cost):
        '''Updates stats.

        Params:
            is_success (bool): algorithm was success flag
            cost (float): algorithm cost
        '''
        async with self._lock:
            self._is_success.append(is_success)
            if is_success:
                self._success_cost.append(cost)
            else:
                self._fail_cost.append(cost)


    async def expected_cost(self, fallback_cost):
        '''Computes expected weighted cost based on success rate and costs.

        Params:
            fallback_cost (float): cost of the fallback algorith

        Returns:
            float|None: total cost or None if not available
        '''
        success_prob = await self.success_prob()
        success_cost = await self.success_cost()
        fail_cost = await self.fail_cost()

        if success_cost is None and success_prob == 0.0:
            success_cost = 0.0

        if fail_cost is None and success_prob == 1.0:
            fail_cost = 0.0

        if success_prob is None or success_cost is None or fail_cost is None:
            return None

        return success_prob * success_cost + ((1 - success_prob) * (fail_cost + fallback_cost))


    async def success_prob(self):
        '''Computes success probability.

        Returns:
            float|None: success probability or None if not available
        '''
        async with self._lock:
            if not self._is_success:
                return None

            # eliminate FPA error
            if True not in self._is_success:
                return 0.0
            if False not in self._is_success:
                return 1.0

            return self._is_success.count(True) / len(self._is_success)


    async def success_cost(self):
        '''Computes success cost.

        Returns:
            float|None: success cost or None if not available
        '''
        async with self._lock:
            if not self._success_cost:
                return None
            return sum(self._success_cost) / len(self._success_cost)


    async def fail_cost(self):
        '''Computes fail cost.

        Returns:
            float|None: fail cost or None if not available
        '''
        async with self._lock:
            if not self._fail_cost:
                return None
            return sum(self._fail_cost) / len(self._fail_cost)
