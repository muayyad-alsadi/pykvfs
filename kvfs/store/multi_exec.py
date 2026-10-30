"""Mutations as optimistic transactions, used when scripts are off (`--sss-mode=no`, or auto without scripts).

Each mutation WATCHes the keys it decides on, reads them, then sends the writes
in MULTI/EXEC. EXEC returns nil when a watched key changed, and the attempt
starts again. Use it where the server refuses scripts.
"""

from __future__ import annotations

from typing import Optional

from . import keys
from .errors import Exists, IsDir, NotDir, NotEmpty, NotFound
from .meta import TYPE_DIR, Meta, encode_child, new_meta, parse_child
from .store import Ref, ROOT, Store, _committed, _now, label
from .wire import Session


def _is_dir(value: bytes) -> bool:
    return parse_child(b"", value).type == TYPE_DIR


class MultiExecStore(Store):
    """The namespace mutations as WATCH/MULTI/EXEC transactions."""

    uses_scripts = False

    async def _init_root(self, uid: int, gid: int) -> None:
        await self._optimistic(lambda s: self._init_attempt(s, uid, gid))

    async def _init_attempt(self, s: Session, uid: int, gid: int) -> object:
        counter = self._counter_key()
        (exists,) = await s.watch_and_read([counter], [b"EXISTS", counter])
        if exists:
            return True
        root = new_meta(keys.ROOT_ID, TYPE_DIR, 0o755, uid, gid, _now())
        committed = await s.transaction([
            [b"SET", counter, keys.ROOT_ID],
            [b"SET", self._meta_key(ROOT.parent_id, ROOT.name), root.to_json()],
        ])
        return _committed(committed, True)

    # ---- create -----------------------------------------------------------

    async def _create(self, parent: Ref, name: bytes, typ: str, mode: int, uid: int, gid: int, data: Optional[bytes]) -> tuple[Ref, Meta]:
        return await self._optimistic(
            lambda s: self._create_attempt(s, parent, name, typ, mode, uid, gid, data)
        )

    async def _create_attempt(self, s: Session, parent: Ref, name: bytes, typ: str, mode: int, uid: int, gid: int, data: Optional[bytes]) -> object:
        parent_meta = self._meta_key(parent.parent_id, parent.name)
        listing = self._listing_key(parent.parent_id, parent.name)
        raw, taken = await s.watch_and_read(
            [parent_meta, listing], [b"GET", parent_meta], [b"HEXISTS", listing, name]
        )
        self._require_parent(raw, parent)
        if taken:
            raise Exists(label(name))
        (inode,) = await s.run([b"INCR", self._counter_key()])
        meta = new_meta(inode, typ, mode & 0o777, uid, gid, _now())
        cmds = [[b"SET", self._meta_key(parent.id, name), meta.to_json()]]
        if data is not None:
            cmds.append([b"SET", self._data_key(parent.id, name), data])
        cmds.append([b"HSET", listing, name, encode_child(typ, inode)])
        cmds.append(self._announce(parent.id, name))
        committed = await s.transaction(cmds)
        meta.size = len(data) if data else 0
        return _committed(committed, (Ref(parent.id, name, inode), meta))

    def _require_parent(self, raw: Optional[bytes], parent: Ref) -> None:
        if raw is None or Meta.from_json(raw).id != parent.id:
            raise NotFound(f"parent directory {parent.id} was removed")

    # ---- unlink, rmdir ----------------------------------------------------

    async def unlink(self, parent: Ref, name: bytes) -> None:
        await self._optimistic(lambda s: self._unlink_attempt(s, parent, name))

    async def _unlink_attempt(self, s: Session, parent: Ref, name: bytes) -> object:
        listing = self._listing_key(parent.parent_id, parent.name)
        meta = self._meta_key(parent.id, name)
        value, exists = await s.watch_and_read(
            [listing, meta], [b"HGET", listing, name], [b"EXISTS", meta]
        )
        if value is None or not exists:
            raise NotFound(label(name))
        if _is_dir(value):
            raise IsDir(label(name))
        committed = await s.transaction([
            [b"DEL", meta],
            [b"DEL", self._data_key(parent.id, name)],
            [b"HDEL", listing, name],
            self._announce(parent.id, name),
        ])
        return _committed(committed, None)

    async def rmdir(self, parent: Ref, name: bytes) -> None:
        await self._optimistic(lambda s: self._rmdir_attempt(s, parent, name))

    async def _rmdir_attempt(self, s: Session, parent: Ref, name: bytes) -> object:
        listing = self._listing_key(parent.parent_id, parent.name)
        meta = self._meta_key(parent.id, name)
        own = self._listing_key(parent.id, name)
        value, exists, children = await s.watch_and_read(
            [listing, meta, own],
            [b"HGET", listing, name],
            [b"EXISTS", meta],
            [b"HLEN", own],
        )
        if value is None or not exists:
            raise NotFound(label(name))
        if not _is_dir(value):
            raise NotDir(label(name))
        if children:
            raise NotEmpty(label(name))
        committed = await s.transaction([
            [b"DEL", meta],
            [b"DEL", own],
            [b"HDEL", listing, name],
            self._announce(parent.id, name),
        ])
        return _committed(committed, None)

    # ---- rename -----------------------------------------------------------

    async def _rename(self, old_parent: Ref, old_name: bytes, new_parent: Ref, new_name: bytes, replace: bool) -> int:
        return await self._optimistic(
            lambda s: self._rename_attempt(s, old_parent, old_name, new_parent, new_name, replace)
        )

    async def _rename_attempt(self, s: Session, old_parent: Ref, old_name: bytes, new_parent: Ref, new_name: bytes, replace: bool) -> object:
        keys_ = self._rename_keys(old_parent, old_name, new_parent, new_name)
        (parent_raw, value, src_exists, data_exists, own_exists, dst_value, dst_children) = await s.watch_and_read(
            keys_["watched"],
            [b"GET", keys_["new_parent_meta"]],
            [b"HGET", keys_["old_listing"], old_name],
            [b"EXISTS", keys_["old_meta"]],
            [b"EXISTS", keys_["old_data"]],
            [b"EXISTS", keys_["old_own"]],
            [b"HGET", keys_["new_listing"], new_name],
            [b"HLEN", keys_["new_own"]],
        )
        self._require_parent(parent_raw, new_parent)
        if value is None or not src_exists:
            raise NotFound(label(old_name))
        if dst_value is not None:
            if not replace:
                raise Exists(label(new_name))
            self._check_replace(value, dst_value, dst_children, new_name)
        cmds = self._rename_writes(keys_, old_parent, old_name, new_parent, new_name, value, dst_value, data_exists, own_exists)
        committed = await s.transaction(cmds)
        return _committed(committed, parse_child(new_name, value).id)

    def _rename_keys(self, old_parent: Ref, old_name: bytes, new_parent: Ref, new_name: bytes) -> dict:
        old_listing = self._listing_key(old_parent.parent_id, old_parent.name)
        new_listing = self._listing_key(new_parent.parent_id, new_parent.name)
        out = {
            "new_parent_meta": self._meta_key(new_parent.parent_id, new_parent.name),
            "old_listing": old_listing,
            "new_listing": new_listing,
            "old_meta": self._meta_key(old_parent.id, old_name),
            "old_data": self._data_key(old_parent.id, old_name),
            "old_own": self._listing_key(old_parent.id, old_name),
            "new_meta": self._meta_key(new_parent.id, new_name),
            "new_data": self._data_key(new_parent.id, new_name),
            "new_own": self._listing_key(new_parent.id, new_name),
        }
        out["watched"] = [
            out["new_parent_meta"], old_listing, out["old_meta"], out["old_data"],
            out["old_own"], new_listing, out["new_own"],
        ]
        return out

    def _check_replace(self, src: bytes, dst: bytes, dst_children: int, name: bytes) -> None:
        src_dir, dst_dir = _is_dir(src), _is_dir(dst)
        if dst_dir and not src_dir:
            raise IsDir(label(name))
        if src_dir and not dst_dir:
            raise NotDir(label(name))
        if dst_dir and dst_children:
            raise NotEmpty(label(name))

    def _rename_writes(self, k: dict, old_parent: Ref, old_name: bytes, new_parent: Ref, new_name: bytes, value: bytes, dst_value: Optional[bytes], data_exists: int, own_exists: int) -> list:
        cmds: list = []
        if dst_value is not None:
            cmds += [[b"DEL", k["new_meta"]], [b"DEL", k["new_data"]], [b"DEL", k["new_own"]]]
        if data_exists:
            cmds.append([b"RENAME", k["old_data"], k["new_data"]])
        cmds.append([b"RENAME", k["old_meta"], k["new_meta"]])
        if own_exists:
            cmds.append([b"RENAME", k["old_own"], k["new_own"]])
        cmds += [
            [b"HDEL", k["old_listing"], old_name],
            [b"HSET", k["new_listing"], new_name, value],
            self._announce(old_parent.id, old_name),
            self._announce(new_parent.id, new_name),
        ]
        return cmds

    # ---- content ------------------------------------------------------------

    async def _write_at(self, ref: Ref, off: int, data: bytes) -> None:
        await self._optimistic(lambda s: self._write_attempt(s, ref, off, data))

    async def _write_attempt(self, s: Session, ref: Ref, off: int, data: bytes) -> object:
        meta = self._meta_key(ref.parent_id, ref.name)
        (exists,) = await s.watch_and_read([meta], [b"EXISTS", meta])
        if not exists:
            raise NotFound(label(ref.name))
        committed = await s.transaction([[b"SETRANGE", self._data_key(ref.parent_id, ref.name), off, data]])
        return _committed(committed, None)

    async def truncate(self, ref: Ref, size: int) -> Meta:
        return await self._optimistic(lambda s: self._truncate_attempt(s, ref, size))

    async def _truncate_attempt(self, s: Session, ref: Ref, size: int) -> object:
        meta = self._meta_key(ref.parent_id, ref.name)
        data = self._data_key(ref.parent_id, ref.name)
        raw, cur = await s.watch_and_read([meta, data], [b"GET", meta], [b"STRLEN", data])
        if raw is None:
            raise NotFound(label(ref.name))
        fresh = Meta.from_json(raw)
        fresh.mtime = fresh.ctime = _now()
        cmds = self._resize_writes(data, size, cur, await self._kept_prefix(s, data, size, cur))
        cmds += [[b"SET", meta, fresh.to_json()], self._announce(ref.parent_id, ref.name)]
        committed = await s.transaction(cmds)
        fresh.size = size
        return _committed(committed, fresh)

    async def _kept_prefix(self, s: Session, data: bytes, size: int, cur: int) -> bytes:
        """Reads the bytes a shrink keeps; only a shrink reads anything."""
        if 0 < size < cur:
            (kept,) = await s.run([b"GETRANGE", data, 0, size - 1])
            return kept
        return b""

    def _resize_writes(self, data: bytes, size: int, cur: int, kept: bytes) -> list:
        """The writes that make the content exactly size bytes long.

        A string cannot shrink in place, so a shorter size rewrites what is kept;
        a longer one pads with a zero byte at the end, which Redis zero-fills.
        """
        if size == cur:
            return []
        if size == 0:
            return [[b"SET", data, b""]]
        if size < cur:
            return [[b"SET", data, kept]]
        return [[b"SETRANGE", data, size - 1, b"\x00"]]
