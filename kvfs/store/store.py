"""Operations on one namespace, at sharding level `namespace` (SPEC.md).

Store holds what both mutation modes share: the keys, every read, the
metadata update, pub/sub, and the optimistic retry loop. The namespace-changing
mutations live in two modules, chosen by `--sss-mode` (modes.py):

- multi_exec.py: an optimistic WATCH/MULTI/EXEC transaction per mutation;
- lua.py: the Go store's namespace-level scripts, sent as Go sends them.

Both write the same keys and the same bytes. Refusals are raised before any write.
This package is the only place that issues Redis commands.
"""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Awaitable, Callable, Optional

from redis.asyncio import ConnectionPool, Redis

from . import keys
from .errors import Busy, NotFound, Placement
from .meta import TYPE_DIR, TYPE_FILE, TYPE_LINK, Child, Meta, parse_child
from .wire import Session

MAX_ATTEMPTS = 64
_CONFLICT = object()


@dataclass(frozen=True)
class Ref:
    """Where an entry lives. parent_id is its owner (the parent's inode), not a name."""

    parent_id: int
    name: bytes
    id: int


ROOT = Ref(keys.ROOT_PARENT_ID, keys.ROOT_NAME, keys.ROOT_ID)


def _now() -> int:
    return int(time.time())


def _committed(committed: Optional[list], result: object) -> object:
    return _CONFLICT if committed is None else result


def label(name: bytes) -> str:
    """A name as error text."""
    return name.decode("utf-8", "replace")


def stamp_modified(now: int) -> Callable[[Meta], None]:
    """The mutation a content change records: the modification time."""

    def stamp(meta: Meta) -> None:
        meta.mtime = now

    return stamp


def check_level(ns: str, level: str) -> None:
    """Refuses a level pykvfs does not implement, or a malformed namespace, before any server call."""
    if level in (keys.DIR, keys.FILE):
        raise NotImplementedError(
            f"sharding level {level!r} is not implemented by pykvfs; only 'namespace' is "
            "(see README.md, Subset of SPEC.md)"
        )
    if level != keys.NAMESPACE:
        raise ValueError(f"unknown sharding level {level!r}: want namespace, dir or file")
    if not keys.valid_namespace(ns):
        raise ValueError(f"invalid namespace {ns!r}: want [A-Za-z0-9-]+, at most {keys.MAX_NAMESPACE_LEN} bytes")


class Store(ABC):
    """Filesystem operations over one namespace on one server."""

    uses_scripts: bool  # True when mutations are sent as the Go store's server-side scripts

    def __init__(self, pool: ConnectionPool, ns: str, level: str = keys.NAMESPACE) -> None:
        check_level(ns, level)
        self._pool = pool
        self._redis = Redis(connection_pool=pool)
        self._ns = ns.encode()
        self._level = level

    async def aclose(self) -> None:
        await self._redis.aclose()
        await self._pool.disconnect()

    # ---- keys -------------------------------------------------------------

    def _key(self, owner: int, name: bytes, suffix: bytes) -> bytes:
        return keys.key(self._ns, self._level, owner, name, suffix)

    def _meta_key(self, owner: int, name: bytes) -> bytes:
        return self._key(owner, name, keys.META)

    def _data_key(self, owner: int, name: bytes) -> bytes:
        return self._key(owner, name, keys.DATA)

    def _listing_key(self, owner: int, name: bytes) -> bytes:
        return self._key(owner, name, keys.DIRECTORY)

    def _counter_key(self) -> bytes:
        return self._key(keys.NAMESPACE_ID, keys.ROOT_NAME, keys.COUNTER)

    def _channel(self) -> bytes:
        return self._key(keys.NAMESPACE_ID, keys.ROOT_NAME, keys.PUBSUB)

    def _announce(self, owner: int, name: bytes) -> list:
        return [b"PUBLISH", self._channel(), keys.invalidate_payload(owner, name)]

    # ---- bootstrap --------------------------------------------------------

    async def bootstrap_root(self, uid: int, gid: int) -> None:
        """Creates the root on first mount. Refuses a namespace written at another level."""
        found = await self._levels_with_counter()
        if not found:
            await self._init_root(uid, gid)
        else:
            self._check_placement(found)
        await self.get_meta(ROOT)

    async def _levels_with_counter(self) -> list[str]:
        pipe = self._redis.pipeline(transaction=False)
        for level in keys.LEVELS:
            pipe.exists(keys.key(self._ns, level, keys.NAMESPACE_ID, keys.ROOT_NAME, keys.COUNTER))
        counts = await pipe.execute()
        return [level for level, count in zip(keys.LEVELS, counts) if count]

    def _check_placement(self, found: list[str]) -> None:
        if len(found) > 1:
            raise Placement(f"namespace {self._ns.decode()!r} has counters at more than one sharding level: refusing to guess")
        if found[0] != self._level:
            raise Placement(f"namespace {self._ns.decode()!r} was initialized at {found[0]!r} level, this mount is {self._level!r}")

    @abstractmethod
    async def _init_root(self, uid: int, gid: int) -> None:
        """Sets the counter to the root's id and writes the root, unless another mount already did."""

    # ---- reads ------------------------------------------------------------

    async def get_meta(self, ref: Ref) -> Meta:
        """One GET: the metadata document of the entry, without its size."""
        raw = await self._redis.get(self._meta_key(ref.parent_id, ref.name))
        if raw is None:
            raise NotFound(label(ref.name))
        return Meta.from_json(raw)

    async def stat(self, ref: Ref) -> Meta:
        """The metadata and the size: GET and STRLEN in one round trip."""
        pipe = self._redis.pipeline(transaction=False)
        pipe.get(self._meta_key(ref.parent_id, ref.name))
        pipe.strlen(self._data_key(ref.parent_id, ref.name))
        raw, size = await pipe.execute()
        if raw is None:
            raise NotFound(label(ref.name))
        meta = Meta.from_json(raw)
        meta.size = size
        return meta

    async def list_children(self, directory: Ref) -> list[Child]:
        """One HGETALL. Type and inode come from the listing; no metadata is read."""
        fields = await self._redis.hgetall(self._listing_key(directory.parent_id, directory.name))
        children = [parse_child(name, value) for name, value in fields.items()]
        return sorted(children, key=lambda child: child.name)

    async def list_with_attrs(self, directory: Ref) -> list[tuple[Child, Meta]]:
        """The listing, then every child's metadata and size in one pipelined round trip."""
        children = await self.list_children(directory)
        if not children:
            return []
        pipe = self._redis.pipeline(transaction=False)
        for child in children:
            pipe.get(self._meta_key(directory.id, child.name))
            pipe.strlen(self._data_key(directory.id, child.name))
        replies = await pipe.execute()
        rows = []
        for child, raw, size in zip(children, replies[0::2], replies[1::2]):
            if raw is not None:
                meta = Meta.from_json(raw)
                meta.size = size
                rows.append((child, meta))
        return rows

    async def read_at(self, ref: Ref, off: int, size: int) -> bytes:
        """One GETRANGE. A read past the end returns what there is."""
        if size <= 0 or off < 0:
            return b""
        return await self._redis.getrange(self._data_key(ref.parent_id, ref.name), off, off + size - 1)

    async def get_data(self, ref: Ref) -> bytes:
        """The whole content: a file's bytes or a symlink's target."""
        return await self._redis.get(self._data_key(ref.parent_id, ref.name)) or b""

    # ---- namespace mutations: one implementation per path (lua or transactions) ----

    async def create_file(self, parent: Ref, name: bytes, mode: int, uid: int, gid: int) -> tuple[Ref, Meta]:
        """A regular file with an empty content key, so presence never means 'empty'."""
        return await self._create(parent, name, TYPE_FILE, mode, uid, gid, data=b"")

    async def mkdir(self, parent: Ref, name: bytes, mode: int, uid: int, gid: int) -> tuple[Ref, Meta]:
        """A directory. It writes no listing key: an empty listing is the absence of a hash."""
        return await self._create(parent, name, TYPE_DIR, mode, uid, gid, data=None)

    async def symlink(self, parent: Ref, name: bytes, target: bytes, uid: int, gid: int) -> tuple[Ref, Meta]:
        """A symlink: the target is its content."""
        return await self._create(parent, name, TYPE_LINK, 0o777, uid, gid, data=target)

    @abstractmethod
    async def _create(self, parent: Ref, name: bytes, typ: str, mode: int, uid: int, gid: int, data: Optional[bytes]) -> tuple[Ref, Meta]:
        """Claims the name, allocates the inode and writes the entry. data None writes no content key."""

    @abstractmethod
    async def unlink(self, parent: Ref, name: bytes) -> None:
        """Removes a file or symlink; a directory raises IsDir."""

    @abstractmethod
    async def rmdir(self, parent: Ref, name: bytes) -> None:
        """Removes an empty directory; a non-directory raises NotDir, a non-empty one NotEmpty."""

    async def rename(self, old_parent: Ref, old_name: bytes, new_parent: Ref, new_name: bytes, replace: bool = True) -> Optional[int]:
        """Moves one entry and returns its unchanged inode, or None when nothing moved.

        With replace, a taken destination is replaced as rename(2) says; without
        it a taken destination raises Exists.
        """
        if old_parent.id == new_parent.id and old_name == new_name:
            return None
        return await self._rename(old_parent, old_name, new_parent, new_name, replace)

    @abstractmethod
    async def _rename(self, old_parent: Ref, old_name: bytes, new_parent: Ref, new_name: bytes, replace: bool) -> int:
        """Moves the entry between two different addresses."""

    async def write_at(self, ref: Ref, off: int, data: bytes) -> None:
        """SETRANGE at off, refused when the entry is gone so a handle cannot recreate its content."""
        if not data:
            return
        await self._write_at(ref, off, data)

    @abstractmethod
    async def _write_at(self, ref: Ref, off: int, data: bytes) -> None:
        """Writes a non-empty range while the entry's metadata exists."""

    @abstractmethod
    async def truncate(self, ref: Ref, size: int) -> Meta:
        """Resizes the content, zero-filling when it grows, stamps it modified, and returns it with its size."""

    # ---- metadata ---------------------------------------------------------

    async def update_meta(self, ref: Ref, mutate: Callable[[Meta], None]) -> Meta:
        """Applies mutate to the metadata document in one watched transaction, and announces it.

        Two round trips in both modes, as in the Go store: WATCH with the reads, then
        MULTI/EXEC. The result carries the content size read alongside the document.
        """
        return await self._optimistic(lambda s: self._update_attempt(s, ref, mutate))

    async def _update_attempt(self, s: Session, ref: Ref, mutate: Callable[[Meta], None]) -> object:
        meta = self._meta_key(ref.parent_id, ref.name)
        raw, size = await s.watch_and_read(
            [meta], [b"GET", meta], [b"STRLEN", self._data_key(ref.parent_id, ref.name)]
        )
        if raw is None:
            raise NotFound(label(ref.name))
        fresh = Meta.from_json(raw)
        mutate(fresh)
        fresh.ctime = _now()
        committed = await s.transaction([[b"SET", meta, fresh.to_json()], self._announce(ref.parent_id, ref.name)])
        fresh.size = size
        return _committed(committed, fresh)

    async def mark_modified(self, ref: Ref) -> Meta:
        """Stamps mtime. Writers call it once, on flush or close, not per write."""
        return await self.update_meta(ref, stamp_modified(_now()))

    # ---- pub/sub and retries ----------------------------------------------

    async def subscribe(self):
        """A pub/sub connection on the namespace channel. The caller closes it."""
        pubsub = self._redis.pubsub()
        await pubsub.subscribe(self._channel())
        return pubsub

    async def _optimistic(self, attempt: Callable[[Session], Awaitable[object]]) -> object:
        for _ in range(MAX_ATTEMPTS):
            async with Session(self._pool) as session:
                outcome = await attempt(session)
            if outcome is not _CONFLICT:
                return outcome
        raise Busy("the same keys kept changing under this operation")
