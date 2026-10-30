"""The FUSE layer: kernel inodes to store refs, and store errors to errno.

The store's inode is the kernel's inode: both are the entry's immutable id, and
the root is inode 1. A rename moves an inode's address (parent id, name) and
never its number, so the kernel's view and the store agree.

Nothing here publishes or calls the kernel from a request handler. Invalidation
runs in watch_invalidations, a separate task that Close (cli.py) cancels.
"""

from __future__ import annotations

import asyncio
import errno
import os
from typing import Optional

import pyfuse3

from .store import ROOT, Ref, Store
from .store.errors import Busy, Exists, IsDir, KvfsError, NotDir, NotEmpty, NotFound, Placement
from .store.keys import parse_invalidate
from .store.meta import Meta

ENTRY_TIMEOUT = 1.0
ATTR_TIMEOUT = 1.0
BLOCK_SIZE = 4096
NANOSECONDS = 1_000_000_000
RENAME_NOREPLACE = 1  # linux/fs.h
RENAME_EXCHANGE = 2
STATFS_BLOCKS = 1 << 30

# One table maps every store error to errno. Add a store error here, nowhere else.
ERRNO = {
    NotFound: errno.ENOENT,
    Exists: errno.EEXIST,
    NotEmpty: errno.ENOTEMPTY,
    NotDir: errno.ENOTDIR,
    IsDir: errno.EISDIR,
    Busy: errno.EBUSY,
    Placement: errno.EINVAL,
}


def errno_for(err: KvfsError) -> int:
    """The errno for a store error. Unmapped errors are EIO, never a guess."""
    return ERRNO.get(type(err), errno.EIO)


def fuse_error(err: KvfsError) -> pyfuse3.FUSEError:
    return pyfuse3.FUSEError(errno_for(err))


def entry_attributes(meta: Meta) -> pyfuse3.EntryAttributes:
    """The kernel's view of one entry, built from its metadata and content length."""
    attr = pyfuse3.EntryAttributes()
    attr.st_ino = meta.id
    attr.st_mode = meta.fuse_mode()
    attr.st_nlink = meta.nlink
    attr.st_uid = meta.uid
    attr.st_gid = meta.gid
    attr.st_size = meta.size
    attr.st_blksize = BLOCK_SIZE
    attr.st_blocks = (meta.size + 511) // 512
    attr.st_atime_ns = meta.atime * NANOSECONDS
    attr.st_mtime_ns = meta.mtime * NANOSECONDS
    attr.st_ctime_ns = meta.ctime * NANOSECONDS
    attr.entry_timeout = ENTRY_TIMEOUT
    attr.attr_timeout = ATTR_TIMEOUT
    return attr


class Inodes:
    """Inode to current address, with the kernel's lookup count for each inode.

    The address is (parent id, name). A rename rewrites it; the number is kept.
    """

    def __init__(self) -> None:
        self._refs: dict[int, Ref] = {ROOT.id: ROOT}
        self._by_address: dict[tuple[int, bytes], int] = {}
        self._lookups: dict[int, int] = {}

    def ref(self, inode: int) -> Ref:
        found = self._refs.get(inode)
        if found is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        return found

    def inode_at(self, parent_id: int, name: bytes) -> Optional[int]:
        return self._by_address.get((parent_id, name))

    def remember(self, ref: Ref) -> None:
        """Records a reply the kernel will keep: one lookup count per reply."""
        self._refs[ref.id] = ref
        self._by_address[(ref.parent_id, ref.name)] = ref.id
        self._lookups[ref.id] = self._lookups.get(ref.id, 0) + 1

    def moved(self, inode: int, new: Ref) -> None:
        old = self._refs.get(inode)
        if old is not None and self._by_address.get((old.parent_id, old.name)) == inode:
            del self._by_address[(old.parent_id, old.name)]
        self._refs[inode] = new
        self._by_address[(new.parent_id, new.name)] = inode

    def forget(self, inode: int, count: int) -> None:
        left = self._lookups.get(inode, 0) - count
        if left > 0 or inode == ROOT.id:
            self._lookups[inode] = left
            return
        self._lookups.pop(inode, None)
        old = self._refs.pop(inode, None)
        if old is not None and self._by_address.get((old.parent_id, old.name)) == inode:
            del self._by_address[(old.parent_id, old.name)]


class Handle:
    """An open file. It holds no content; every write goes straight to the store."""

    def __init__(self, ref: Ref) -> None:
        self.ref = ref
        self.dirty = False


class Operations(pyfuse3.Operations):
    """The kernel's operations, each one or a few store calls."""

    enable_writeback_cache = False

    def __init__(self, store: Store) -> None:
        super().__init__()
        self._store = store
        self._inodes = Inodes()
        self._handles: dict[int, Handle] = {}
        self._dirs: dict[int, dict] = {}
        self._next_fh = 0

    def _new_fh(self) -> int:
        self._next_fh += 1
        return self._next_fh

    async def lookup(self, parent_inode, name, ctx=None):
        parent = self._inodes.ref(parent_inode)
        try:
            meta = await self._store.stat(Ref(parent.id, name, 0))
        except KvfsError as err:
            raise fuse_error(err) from err
        ref = Ref(parent.id, name, meta.id)
        self._inodes.remember(ref)
        return entry_attributes(meta)

    async def getattr(self, inode, ctx=None):
        ref = self._inodes.ref(inode)
        try:
            return entry_attributes(await self._store.stat(ref))
        except KvfsError as err:
            raise fuse_error(err) from err

    async def setattr(self, inode, attr, fields, fh, ctx):
        ref = self._inodes.ref(inode)
        try:
            return entry_attributes(await self._apply_setattr(ref, attr, fields))
        except KvfsError as err:
            raise fuse_error(err) from err

    async def _apply_setattr(self, ref: Ref, attr, fields) -> Meta:
        """Truncate first, then one watched update; each returns the attributes it left."""
        resized = None
        if fields.update_size:
            resized = await self._store.truncate(ref, attr.st_size)
        changes = _attribute_changes(attr, fields)
        if changes:
            return await self._store.update_meta(ref, changes)
        if resized is not None:
            return resized
        return await self._store.stat(ref)

    async def opendir(self, inode, ctx):
        ref = self._inodes.ref(inode)
        fh = self._new_fh()
        self._dirs[fh] = {"ref": ref, "rows": None}
        return fh

    async def readdir(self, fh, start_id, token):
        state = self._dirs[fh]
        if state["rows"] is None:
            state["rows"] = await self._snapshot(state["ref"])
        for index, (meta, name) in enumerate(state["rows"], start=1):
            if index <= start_id:
                continue
            if not pyfuse3.readdir_reply(token, name, entry_attributes(meta), index):
                return
            self._inodes.remember(Ref(state["ref"].id, name, meta.id))

    async def _snapshot(self, directory: Ref) -> list:
        try:
            rows = await self._store.list_with_attrs(directory)
        except KvfsError as err:
            raise fuse_error(err) from err
        return [(meta, child.name) for child, meta in rows]

    async def releasedir(self, fh):
        self._dirs.pop(fh, None)

    async def open(self, inode, flags, ctx):
        ref = self._inodes.ref(inode)
        try:
            meta = await self._store.stat(ref)
            if meta.type == "dir":
                raise pyfuse3.FUSEError(errno.EISDIR)
            if flags & os.O_TRUNC:
                await self._store.truncate(ref, 0)
        except KvfsError as err:
            raise fuse_error(err) from err
        fh = self._new_fh()
        self._handles[fh] = Handle(ref)
        return pyfuse3.FileInfo(fh=fh)

    async def create(self, parent_inode, name, mode, flags, ctx):
        parent = self._inodes.ref(parent_inode)
        try:
            ref, meta = await self._store.create_file(parent, name, mode, ctx.uid, ctx.gid)
        except KvfsError as err:
            raise fuse_error(err) from err
        self._inodes.remember(ref)
        fh = self._new_fh()
        self._handles[fh] = Handle(ref)
        return pyfuse3.FileInfo(fh=fh), entry_attributes(meta)

    async def read(self, fh, off, size):
        try:
            return await self._store.read_at(self._handles[fh].ref, off, size)
        except KvfsError as err:
            raise fuse_error(err) from err

    async def write(self, fh, off, buf):
        handle = self._handles[fh]
        try:
            await self._store.write_at(handle.ref, off, buf)
        except KvfsError as err:
            raise fuse_error(err) from err
        handle.dirty = True
        return len(buf)

    async def flush(self, fh):
        handle = self._handles[fh]
        if not handle.dirty:
            return
        try:
            await self._store.mark_modified(handle.ref)
        except KvfsError as err:
            raise fuse_error(err) from err
        handle.dirty = False

    async def fsync(self, fh, datasync):
        await self.flush(fh)

    async def release(self, fh):
        try:
            await self.flush(fh)
        finally:
            self._handles.pop(fh, None)

    async def mkdir(self, parent_inode, name, mode, ctx):
        parent = self._inodes.ref(parent_inode)
        try:
            ref, meta = await self._store.mkdir(parent, name, mode, ctx.uid, ctx.gid)
        except KvfsError as err:
            raise fuse_error(err) from err
        self._inodes.remember(ref)
        return entry_attributes(meta)

    async def symlink(self, parent_inode, name, target, ctx):
        parent = self._inodes.ref(parent_inode)
        try:
            ref, meta = await self._store.symlink(parent, name, target, ctx.uid, ctx.gid)
        except KvfsError as err:
            raise fuse_error(err) from err
        self._inodes.remember(ref)
        return entry_attributes(meta)

    async def readlink(self, inode, ctx):
        ref = self._inodes.ref(inode)
        try:
            return await self._store.get_data(ref)
        except KvfsError as err:
            raise fuse_error(err) from err

    async def unlink(self, parent_inode, name, ctx):
        await self._remove(parent_inode, name, self._store.unlink)

    async def rmdir(self, parent_inode, name, ctx):
        await self._remove(parent_inode, name, self._store.rmdir)

    async def _remove(self, parent_inode, name, remove) -> None:
        parent = self._inodes.ref(parent_inode)
        try:
            await remove(parent, name)
        except KvfsError as err:
            raise fuse_error(err) from err

    async def rename(self, parent_inode_old, name_old, parent_inode_new, name_new, flags, ctx):
        if flags & RENAME_EXCHANGE:
            raise pyfuse3.FUSEError(errno.EINVAL)
        old_parent = self._inodes.ref(parent_inode_old)
        new_parent = self._inodes.ref(parent_inode_new)
        replace = not flags & RENAME_NOREPLACE
        try:
            inode = await self._store.rename(old_parent, name_old, new_parent, name_new, replace=replace)
        except KvfsError as err:
            raise fuse_error(err) from err
        if inode is not None:
            self._inodes.moved(inode, Ref(new_parent.id, name_new, inode))

    async def statfs(self, ctx):
        data = pyfuse3.StatvfsData()
        data.f_bsize = data.f_frsize = BLOCK_SIZE
        data.f_blocks = data.f_bfree = data.f_bavail = STATFS_BLOCKS
        data.f_files = data.f_ffree = data.f_favail = STATFS_BLOCKS
        data.f_namemax = 255
        return data

    async def forget(self, inode_list):
        for inode, count in inode_list:
            self._inodes.forget(inode, count)

    def locate(self, payload: bytes) -> tuple[int, bytes, Optional[int]]:
        """Resolves a published address to (parent inode, name, child inode or None). Loop-only."""
        owner, name = parse_invalidate(payload)
        return owner, name, self._inodes.inode_at(owner, name)


def notify_kernel(owner: int, name: bytes, inode: Optional[int]) -> None:
    """Drops the kernel's cached entry and attributes. Runs in a worker thread, never on the loop.

    invalidate_* writes to /dev/fuse and can wait for the kernel, which may hold
    a directory lock for a request this loop is still serving. On the loop that
    is a deadlock; in a thread the loop keeps answering and the wait ends.
    """
    _drop_quietly(pyfuse3.invalidate_entry, owner, name)
    if inode is not None:
        _drop_quietly(pyfuse3.invalidate_inode, inode)


def _drop_quietly(drop, *args) -> None:
    """A kernel entry that was never cached is already dropped: ENOENT is not an error."""
    try:
        drop(*args)
    except OSError:
        pass


def _attribute_changes(attr, fields):
    """One mutation of the metadata document, or None when no attribute is asked to change."""
    wanted = []
    if fields.update_mode:
        wanted.append(lambda m: setattr(m, "mode", attr.st_mode & 0o777))
    if fields.update_uid:
        wanted.append(lambda m: setattr(m, "uid", attr.st_uid))
    if fields.update_gid:
        wanted.append(lambda m: setattr(m, "gid", attr.st_gid))
    if fields.update_atime:
        wanted.append(lambda m: setattr(m, "atime", attr.st_atime_ns // NANOSECONDS))
    if fields.update_mtime:
        wanted.append(lambda m: setattr(m, "mtime", attr.st_mtime_ns // NANOSECONDS))
    if not wanted:
        return None

    def apply(meta: Meta) -> None:
        for change in wanted:
            change(meta)

    return apply


async def watch_invalidations(store: Store, ops: Operations) -> None:
    """Feeds the namespace channel to the kernel until cancelled. One owner, one loop."""
    pubsub = await store.subscribe()
    try:
        async for message in pubsub.listen():
            if message.get("type") == "message":
                await asyncio.to_thread(notify_kernel, *ops.locate(message["data"]))
    finally:
        await pubsub.aclose()
