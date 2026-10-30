"""Mutations as server-side scripts, used when scripts are on (`--sss-mode=yes`, or auto with scripts).

The scripts are the Go store's namespace-level scripts, byte for byte, sent the
way the Go store sends them: EVAL with the whole body, the same keys in the same
order, the same arguments. Each one checks and writes in one round trip. The
metadata update stays a watched transaction, as it is in Go (store.py).
"""

from __future__ import annotations

from typing import Optional

from . import keys
from .errors import Exists, IsDir, NotDir, NotEmpty, NotFound
from .meta import TYPE_DIR, Meta, new_meta, parse_child, type_code
from .store import ROOT, Ref, Store, _now, label, stamp_modified

# Go: internal/store/store.go, bootstrapScript.
BOOTSTRAP = r"""
if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
redis.call('SET', KEYS[1], 1)
redis.call('SET', KEYS[2], ARGV[1])
return 1
"""

# Go: internal/store/store.go, createScript.
CREATE = r"""
local pm = redis.call('GET', KEYS[6])
if not pm or string.match(pm, '^{"id":(%d+)[,}]') ~= ARGV[7] then return -1 end
if redis.call('HEXISTS', KEYS[1], ARGV[1]) == 1 then return 0 end
local id = redis.call('INCR', KEYS[2])
local meta = string.gsub(ARGV[2], '"id":0', '"id":' .. id, 1)
redis.call('SET', KEYS[3], meta)
if ARGV[6] == '1' then redis.call('SET', KEYS[4], ARGV[3]) end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[4] .. ':' .. id)
redis.call('PUBLISH', KEYS[5], ARGV[5])
return id
"""

# Go: internal/store/store.go, unlinkScript.
UNLINK = r"""
local v = redis.call('HGET', KEYS[3], ARGV[1])
if not v then return -1 end
if string.sub(v, 1, 1) == 'd' then return -2 end
if redis.call('EXISTS', KEYS[1]) == 0 then return -1 end
redis.call('DEL', KEYS[1])
redis.call('DEL', KEYS[2])
redis.call('HDEL', KEYS[3], ARGV[1])
redis.call('PUBLISH', KEYS[4], ARGV[2])
return 1
"""

# Go: internal/store/store.go, rmdirScript.
RMDIR = r"""
local v = redis.call('HGET', KEYS[3], ARGV[1])
if not v then return -1 end
if string.sub(v, 1, 1) ~= 'd' then return -2 end
if redis.call('EXISTS', KEYS[1]) == 0 then return -1 end
if redis.call('HLEN', KEYS[2]) > 0 then return 0 end
redis.call('DEL', KEYS[1])
redis.call('DEL', KEYS[2])
redis.call('HDEL', KEYS[3], ARGV[1])
redis.call('PUBLISH', KEYS[4], ARGV[2])
return 1
"""

# Go: internal/store/store.go, renameScript.
RENAME = r"""
local pm = redis.call('GET', KEYS[10])
if not pm or string.match(pm, '^{"id":(%d+)[,}]') ~= ARGV[6] then return {'missing', ''} end
local v = redis.call('HGET', KEYS[7], ARGV[1])
if not v then return {'missing', ''} end
if redis.call('EXISTS', KEYS[1]) == 0 then return {'missing', ''} end
local dv = redis.call('HGET', KEYS[8], ARGV[2])
if dv then
  if ARGV[5] ~= '1' then return {'exists', ''} end
  local src = string.sub(v, 1, 1) == 'd'
  local dst = string.sub(dv, 1, 1) == 'd'
  if dst and not src then return {'isdir', ''} end
  if src and not dst then return {'notdir', ''} end
  if dst and redis.call('HLEN', KEYS[6]) > 0 then return {'notempty', ''} end
  redis.call('DEL', KEYS[4], KEYS[5], KEYS[6])
end
redis.call('RENAME', KEYS[1], KEYS[4])
if redis.call('EXISTS', KEYS[2]) == 1 then redis.call('RENAME', KEYS[2], KEYS[5]) end
if redis.call('EXISTS', KEYS[3]) == 1 then redis.call('RENAME', KEYS[3], KEYS[6]) end
redis.call('HDEL', KEYS[7], ARGV[1])
redis.call('HSET', KEYS[8], ARGV[2], v)
redis.call('PUBLISH', KEYS[9], ARGV[3])
redis.call('PUBLISH', KEYS[9], ARGV[4])
return {'ok', v}
"""

# Go: internal/store/content.go, writeAtScript.
WRITE_AT = r"""
if redis.call('EXISTS', KEYS[1]) == 0 then return -1 end
return redis.call('SETRANGE', KEYS[2], ARGV[1], ARGV[2])
"""

# Go: internal/store/content.go, truncateScript.
TRUNCATE = r"""
if redis.call('EXISTS', KEYS[1]) == 0 then return -1 end
local n = tonumber(ARGV[1])
local cur = redis.call('STRLEN', KEYS[2])
if n == 0 then
  redis.call('SET', KEYS[2], '')
elseif n < cur then
  redis.call('SET', KEYS[2], redis.call('GETRANGE', KEYS[2], 0, n - 1))
elseif n > cur then
  redis.call('SETRANGE', KEYS[2], n - 1, '\0')
end
return n
"""

# The scripts' verdicts, mapped to store errors. Anything else is a miss.
UNLINK_REFUSALS = {-2: IsDir}
RMDIR_REFUSALS = {0: NotEmpty, -2: NotDir}
RENAME_REFUSALS = {b"exists": Exists, b"isdir": IsDir, b"notdir": NotDir, b"notempty": NotEmpty}

REPLACE = {True: b"1", False: b"0"}


def _decimal(n: int) -> bytes:
    return str(n).encode()


class LuaStore(Store):
    """The namespace mutations as one script each."""

    uses_scripts = True

    async def _eval(self, script: str, keys_: list[bytes], args: list[bytes]) -> object:
        return await self._redis.execute_command("EVAL", script, len(keys_), *keys_, *args)

    async def _init_root(self, uid: int, gid: int) -> None:
        root = new_meta(keys.ROOT_ID, TYPE_DIR, 0o755, uid, gid, _now())
        await self._eval(
            BOOTSTRAP,
            [self._counter_key(), self._meta_key(ROOT.parent_id, ROOT.name)],
            [root.to_json()],
        )

    async def _create(self, parent: Ref, name: bytes, typ: str, mode: int, uid: int, gid: int, data: Optional[bytes]) -> tuple[Ref, Meta]:
        template = new_meta(0, typ, mode & 0o777, uid, gid, _now())
        inode = await self._eval(
            CREATE,
            [
                self._listing_key(parent.parent_id, parent.name),
                self._counter_key(),
                self._meta_key(parent.id, name),
                self._data_key(parent.id, name),
                self._channel(),
                self._meta_key(parent.parent_id, parent.name),
            ],
            [
                name,
                template.to_json(),
                data or b"",
                type_code(typ),
                keys.invalidate_payload(parent.id, name),
                b"0" if data is None else b"1",
                _decimal(parent.id),
            ],
        )
        if inode < 0:
            raise NotFound(f"parent directory {parent.id} was removed")
        if inode == 0:
            raise Exists(label(name))
        template.id = inode
        template.size = len(data) if data else 0
        return Ref(parent.id, name, inode), template

    async def unlink(self, parent: Ref, name: bytes) -> None:
        verdict = await self._eval(
            UNLINK,
            [
                self._meta_key(parent.id, name),
                self._data_key(parent.id, name),
                self._listing_key(parent.parent_id, parent.name),
                self._channel(),
            ],
            [name, keys.invalidate_payload(parent.id, name)],
        )
        if verdict != 1:
            raise UNLINK_REFUSALS.get(verdict, NotFound)(label(name))

    async def rmdir(self, parent: Ref, name: bytes) -> None:
        verdict = await self._eval(
            RMDIR,
            [
                self._meta_key(parent.id, name),
                self._listing_key(parent.id, name),
                self._listing_key(parent.parent_id, parent.name),
                self._channel(),
            ],
            [name, keys.invalidate_payload(parent.id, name)],
        )
        if verdict != 1:
            raise RMDIR_REFUSALS.get(verdict, NotFound)(label(name))

    async def _rename(self, old_parent: Ref, old_name: bytes, new_parent: Ref, new_name: bytes, replace: bool) -> int:
        verdict, value = await self._eval(
            RENAME,
            [
                self._meta_key(old_parent.id, old_name),
                self._data_key(old_parent.id, old_name),
                self._listing_key(old_parent.id, old_name),
                self._meta_key(new_parent.id, new_name),
                self._data_key(new_parent.id, new_name),
                self._listing_key(new_parent.id, new_name),
                self._listing_key(old_parent.parent_id, old_parent.name),
                self._listing_key(new_parent.parent_id, new_parent.name),
                self._channel(),
                self._meta_key(new_parent.parent_id, new_parent.name),
            ],
            [
                old_name,
                new_name,
                keys.invalidate_payload(old_parent.id, old_name),
                keys.invalidate_payload(new_parent.id, new_name),
                REPLACE[replace],
                _decimal(new_parent.id),
            ],
        )
        if verdict == b"ok":
            return parse_child(new_name, value).id
        if verdict in RENAME_REFUSALS:
            raise RENAME_REFUSALS[verdict](label(new_name))
        raise NotFound(label(old_name))

    async def _write_at(self, ref: Ref, off: int, data: bytes) -> None:
        written = await self._eval(
            WRITE_AT,
            [self._meta_key(ref.parent_id, ref.name), self._data_key(ref.parent_id, ref.name)],
            [_decimal(off), data],
        )
        if written < 0:
            raise NotFound(label(ref.name))

    async def truncate(self, ref: Ref, size: int) -> Meta:
        resized = await self._eval(
            TRUNCATE,
            [self._meta_key(ref.parent_id, ref.name), self._data_key(ref.parent_id, ref.name)],
            [_decimal(size)],
        )
        if resized < 0:
            raise NotFound(label(ref.name))
        return await self.update_meta(ref, stamp_modified(_now()))
