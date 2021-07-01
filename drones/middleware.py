import logging
from typing import Callable, Awaitable

from starlette.middleware.base import BaseHTTPMiddleware
from fastapi import Request, Response

from .settings import BUGOUT_DRONES_TOKEN, BUGOUT_DRONES_TOKEN_HEADER

logger = logging.getLogger(__name__)


class DronesAuthMiddleware(BaseHTTPMiddleware):
    """
    Checks the authorization header on the request. If it represents a verified Brood user,
    create another request and get groups user belongs to, after this
    adds a brood_user attribute to the request.state. Otherwise raises a 403 error.
    """

    def __init__(self, app):
        super().__init__(app)

    async def dispatch(
        self, request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ):
        drones_token = request.headers.get(BUGOUT_DRONES_TOKEN_HEADER)
        if drones_token is None:
            return Response(
                status_code=403, content="No authorization header passed with request"
            )

        if drones_token != BUGOUT_DRONES_TOKEN:
            return Response(status_code=403, content="Access denied wrong credentials",)

        return await call_next(request)
