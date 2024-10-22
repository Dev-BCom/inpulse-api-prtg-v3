import asyncio
import logging

class ActiveRequestCounter:
    def __init__(self, max_concurrent_requests):
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)
        self.active_requests = 0
        self.lock = asyncio.Lock()

    async def __aenter__(self):
        await self.semaphore.acquire()
        async with self.lock:
            self.active_requests += 1
            logging.info(f"Active requests: {self.active_requests}")
        return self

    async def __aexit__(self, exc_type, exc, tb):
        async with self.lock:
            self.active_requests -= 1
            logging.info(f"Active requests: {self.active_requests}")
        self.semaphore.release()
