# @Author : taojinmin
# @Time : 2023/2/10 17:29
from aiojobs import Scheduler
import aiojobs
import inspect
import asyncio


class AsyncJobSchedular(Scheduler):
    def __init__(self, limit: int = 100, pending_limit: int = 200):
        super().__init__(limit=limit, pending_limit=pending_limit)

    async def wrapper(self, coro, callback):
        result = await coro
        if inspect.iscoroutinefunction(callback):
            result = await callback(result)
        elif inspect.isfunction(callback):
            result = callback(result)
        elif inspect.ismethod(callback):
            result = callback(result)
        return result

    async def start(self, coro, callback=None):
        new_coro = self.wrapper(coro, callback)
        await super().spawn(new_coro)

    async def block_until_finish_all_jobs(self):
        while self.pending_count or self.active_count:
            await asyncio.sleep(0.005)
