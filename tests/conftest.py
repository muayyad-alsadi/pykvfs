"""Shared fixtures. Tests run against a real Redis server (AGENTS.md: no fakes).

KVFS_TEST_REDIS=redis://host:port/db points at an existing server. Otherwise a
redis-server (or valkey-server) is started on a free port for the session.
"""

from __future__ import annotations

import contextlib
import os
import shutil
import socket
import subprocess
import time
import uuid
from collections.abc import AsyncIterator, Iterator

import pytest

from kvfs.store import Metrics, Store, make_pool
from kvfs.store import open_store as make_store


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _server_binary() -> str:
    for name in ("redis-server", "valkey-server"):
        found = shutil.which(name)
        if found:
            return found
    pytest.skip("no redis-server or valkey-server on PATH")
    raise AssertionError("unreachable")


@pytest.fixture(scope="session")
def redis_url(tmp_path_factory) -> Iterator[str]:
    external = os.environ.get("KVFS_TEST_REDIS")
    if external:
        yield external
        return
    port = _free_port()
    workdir = tmp_path_factory.mktemp("redis")
    proc = subprocess.Popen(
        [_server_binary(), "--port", str(port), "--save", "", "--appendonly", "no", "--dir", str(workdir)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    _wait_for_port(port)
    try:
        yield f"redis://127.0.0.1:{port}/0"
    finally:
        proc.terminate()
        proc.wait(timeout=10)


def _wait_for_port(port: int, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with socket.socket() as sock:
            if sock.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(0.05)
    raise RuntimeError(f"redis did not start on port {port}")


@pytest.fixture
def namespace() -> str:
    """A namespace no other test uses, so tests never share keys."""
    return "t" + uuid.uuid4().hex[:12]


@pytest.fixture(params=["no", "yes"])
def mode(request) -> str:
    """Each store test runs once per --sss-mode value that uses a real server path: no (transactions) and yes (scripts)."""
    return request.param


@contextlib.asynccontextmanager
async def open_store(url: str, ns: str, mode: str = "no", level: str = "namespace") -> AsyncIterator[tuple[Store, Metrics]]:
    """A bootstrapped store and its round-trip counter. Tests drive it with asyncio.run."""
    metrics = Metrics()
    pool = make_pool(url, metrics)
    try:
        st = await make_store(pool, ns, level, mode)
        await st.bootstrap_root(uid=1000, gid=1000)
        yield st, metrics
    finally:
        await pool.disconnect()
