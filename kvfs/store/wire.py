"""One pinned connection per optimistic transaction, and round-trip counting.

A WATCH has to stay on the connection that reads the watched keys until EXEC,
so a Session takes one connection from the pool and holds it. Every send to the
server goes through CountedConnection, so round trips are counted where they
happen, not estimated.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from redis.asyncio import Connection, ConnectionPool
from redis.exceptions import ResponseError


@dataclass
class Metrics:
    """Round trips sent through pools made by make_pool."""

    round_trips: int = 0


def make_pool(url: str, metrics: Metrics, **kwargs) -> ConnectionPool:
    """A single-server pool whose connections count every round trip into metrics."""

    class CountedConnection(Connection):
        async def send_packed_command(self, command, check_health=True):
            metrics.round_trips += 1
            return await super().send_packed_command(command, check_health=check_health)

    return ConnectionPool.from_url(
        url, connection_class=CountedConnection, decode_responses=False, **kwargs
    )


def _raise_first(replies: list) -> list:
    for reply in replies:
        if isinstance(reply, ResponseError):
            raise reply
    return replies


class Session:
    """One pooled connection, held from the first WATCH until EXEC or UNWATCH.

    Each method is one round trip. watch_and_read sends WATCH and the reads in a
    single write, transaction sends MULTI, the writes and EXEC in a single write.
    """

    def __init__(self, pool: ConnectionPool) -> None:
        self._pool = pool
        self._conn = None
        self._watching = False

    async def __aenter__(self) -> Session:
        self._conn = await self._pool.get_connection("WATCH")
        return self

    async def __aexit__(self, *_exc) -> None:
        try:
            if self._watching:
                await self._send([[b"UNWATCH"]])
        finally:
            await self._pool.release(self._conn)

    async def _send(self, commands: list[list]) -> list:
        conn = self._conn
        await conn.send_packed_command(conn.pack_commands(commands))
        replies: list = []
        for _ in commands:
            try:
                replies.append(await conn.read_response())
            except ResponseError as err:
                replies.append(err)
        return replies

    async def watch_and_read(self, watched: list[bytes], *reads: list) -> list:
        """WATCH the keys, then run the reads, in one round trip. Returns the read replies."""
        replies = _raise_first(await self._send([[b"WATCH", *watched], *reads]))
        self._watching = True
        return replies[1:]

    async def run(self, *commands: list) -> list:
        """Runs commands on the watched connection, in one round trip."""
        return _raise_first(await self._send(list(commands)))

    async def transaction(self, commands: list[list]) -> Optional[list]:
        """MULTI, the commands, EXEC in one round trip. None when a watched key changed."""
        replies = await self._send([[b"MULTI"], *commands, [b"EXEC"]])
        self._watching = False
        _raise_first(replies)
        committed = replies[-1]
        if committed is not None:
            _raise_first(committed)
        return committed
