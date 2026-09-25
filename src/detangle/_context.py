"""Context variables shared by the loop and the network simulation."""

from __future__ import annotations

from contextvars import ContextVar

#: Simulated host the current task runs on (``None`` = the default host).
current_host: ContextVar[str | None] = ContextVar("detangle_current_host", default=None)

DEFAULT_HOST = "localhost"


def host_name() -> str:
    return current_host.get() or DEFAULT_HOST
