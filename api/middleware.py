# ============================================================================
# File: api/middleware.py
# ============================================================================

import time
import uuid
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response


class RequestContextMiddleware(BaseHTTPMiddleware):
    """
    Injects:
    - request_id
    - api_latency_ms
    """

    async def dispatch(self, request: Request, call_next):
        request_id = str(uuid.uuid4())
        start_time = time.perf_counter()

        # Attach request_id to request state
        request.state.request_id = request_id

        response: Response = await call_next(request)

        latency_ms = int((time.perf_counter() - start_time) * 1000)

        # Attach headers (useful for debugging)
        response.headers["X-Request-ID"] = request_id
        response.headers["X-API-Latency-ms"] = str(latency_ms)



        return response
