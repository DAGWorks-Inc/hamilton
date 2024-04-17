import time
from logging import getLogger

from asgiref.sync import iscoroutinefunction, markcoroutinefunction

logger = getLogger(__name__)


class TimingMiddleware:
    async_capable = True
    sync_capable = False

    def __init__(self, get_response):
        self.get_response = get_response
        if iscoroutinefunction(self.get_response):
            markcoroutinefunction(self)
        self.iscoroutine = iscoroutinefunction(self.get_response)

    async def __call__(self, request):
        t0 = time.time()
        url = request.get_full_path()
        response = await self.get_response(request)
        t1 = time.time()
        is_sync = "Sync" if not self.iscoroutine else "Async"
        logger.warning(f"[{is_sync}] Request : {url} took {t1 - t0} seconds.")
        # Some logic ...
        return response
