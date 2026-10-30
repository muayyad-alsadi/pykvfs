"""Store behavior against a real server, at namespace sharding, with and without server-side scripts.

Each test drives the async store with asyncio.run. Round trips are read from the
Metrics counter that the pool's connections increment, not estimated.
"""

import asyncio
import contextlib
import json
import uuid

import pytest
import redis.asyncio as aioredis

from kvfs.store import (
    Busy,
    Exists,
    IsDir,
    LuaStore,
    MultiExecStore,
    NotDir,
    NotEmpty,
    NotFound,
    Placement,
    ROOT,
    Ref,
    ScriptsRefused,
    Store,
    make_pool,
    open_store as make_store,
)
from kvfs.store.errors import KvfsError
from kvfs.store.meta import Meta
from kvfs.store.store import _CONFLICT
from kvfs.store.wire import Metrics

from .conftest import open_store


def run(coro):
    return asyncio.run(coro)


async def _measure(st: Store, metrics: Metrics, call):
    """The result of one awaited call, and the round trips it cost.

    A warm-up read first, so the pool's connection is already open and the
    count holds only what the call itself sends.
    """
    await st.get_meta(ROOT)
    metrics.round_trips = 0
    result = await call()
    return result, metrics.round_trips


def test_bootstrap_writes_the_documented_keys(redis_url, namespace, mode):
    async def scenario():
        async with open_store(redis_url, namespace, mode):
            raw = aioredis.from_url(redis_url)
            try:
                assert await raw.get(f"kvfs/{{{namespace}}}/0/./counter") == b"1"
                root = Meta.from_json(await raw.get(f"kvfs/{{{namespace}}}/0/./meta"))
                assert (root.id, root.type, root.mode) == (1, "dir", 0o755)
                assert await raw.exists(f"kvfs/{{{namespace}}}/0/./dir") == 0
            finally:
                await raw.aclose()

    run(scenario())


def test_bootstrap_is_idempotent(redis_url, namespace, mode):
    async def scenario():
        async with open_store(redis_url, namespace, mode):
            pass
        async with open_store(redis_url, namespace, mode):
            raw = aioredis.from_url(redis_url)
            try:
                assert await raw.get(f"kvfs/{{{namespace}}}/0/./counter") == b"1"
            finally:
                await raw.aclose()

    run(scenario())


def test_create_file_writes_data_listing_and_announces(redis_url, namespace, mode):
    async def scenario():
        async with open_store(redis_url, namespace, mode) as (st, _):
            pubsub = await st.subscribe()
            await pubsub.get_message(timeout=0.5)
            ref, meta = await st.create_file(ROOT, b"README.md", 0o644, 1000, 1000)
            raw = aioredis.from_url(redis_url)
            try:
                assert ref.id == 2 and meta.size == 0
                assert await raw.get(f"kvfs/{{{namespace}}}/1/README.md/data") == b""
                assert await raw.hget(f"kvfs/{{{namespace}}}/0/./dir", "README.md") == b"f:2"
                assert await _published(pubsub) == [b"1/README.md"]
            finally:
                await pubsub.aclose()
                await raw.aclose()

    run(scenario())


def test_create_refuses_a_taken_name_without_burning_an_id(redis_url, namespace, mode):
    async def scenario():
        async with open_store(redis_url, namespace, mode) as (st, _):
            await st.create_file(ROOT, b"a", 0o644, 0, 0)
            with pytest.raises(Exists):
                await st.create_file(ROOT, b"a", 0o644, 0, 0)
            ref, _ = await st.create_file(ROOT, b"b", 0o644, 0, 0)
            assert ref.id == 3

    run(scenario())


def test_create_under_a_removed_directory_is_refused(redis_url, namespace, mode):
    async def scenario():
        async with open_store(redis_url, namespace, mode) as (st, _):
            d, _ = await st.mkdir(ROOT, b"d", 0o755, 0, 0)
            await st.rmdir(ROOT, b"d")
            with pytest.raises(NotFound):
                await st.create_file(d, b"orphan", 0o644, 0, 0)
            ref, _ = await st.create_file(ROOT, b"next", 0o644, 0, 0)
            assert ref.id == 3

    run(scenario())


def test_readdir_is_one_round_trip(redis_url, namespace, mode):
    async def scenario():
        async with open_store(redis_url, namespace, mode) as (st, metrics):
            for i in range(20):
                await st.create_file(ROOT, f"f{i}".encode(), 0o644, 0, 0)
            children, rt = await _measure(st, metrics, lambda: st.list_children(ROOT))
            assert len(children) == 20 and rt == 1

    run(scenario())


def test_readdir_with_attributes_is_two_round_trips(redis_url, namespace, mode):
    async def scenario():
        async with open_store(redis_url, namespace, mode) as (st, metrics):
            for i in range(20):
                await st.create_file(ROOT, f"f{i}".encode(), 0o644, 0, 0)
            rows, rt = await _measure(st, metrics, lambda: st.list_with_attrs(ROOT))
            assert len(rows) == 20 and rt == 2

    run(scenario())


def test_stat_and_lookup_are_one_round_trip(redis_url, namespace, mode):
    async def scenario():
        async with open_store(redis_url, namespace, mode) as (st, metrics):
            ref, _ = await st.create_file(ROOT, b"f", 0o644, 0, 0)
            meta, rt = await _measure(st, metrics, lambda: st.stat(ref))
            assert meta.id == ref.id and rt == 1

    run(scenario())


def test_read_is_one_round_trip(redis_url, namespace, mode):
    async def scenario():
        async with open_store(redis_url, namespace, mode) as (st, metrics):
            ref, _ = await st.create_file(ROOT, b"f", 0o644, 0, 0)
            await st.write_at(ref, 0, b"hello world")
            data, rt = await _measure(st, metrics, lambda: st.read_at(ref, 6, 5))
            assert data == b"world" and rt == 1

    run(scenario())


# Round trips per mutation: (operation, sss=no, sss=yes). Scripts are one EVAL each;
# truncate is EVAL plus the watched metadata update, as in the Go store.
MUTATION_COSTS = [
    ("create", 3, 1),
    ("mkdir", 3, 1),
    ("write", 2, 1),
    ("truncate shrink", 3, 3),
    ("truncate grow", 2, 3),
    ("update metadata", 2, 2),
    ("rename", 2, 1),
    ("unlink", 2, 1),
    ("rmdir", 2, 1),
]

COST_COLUMN = {"no": 0, "yes": 1}


def _mutation(st: Store, op: str, file: Ref):
    """The call for one row of MUTATION_COSTS, against a store holding file f (6 bytes) and empty dir d."""
    calls = {
        "create": lambda: st.create_file(ROOT, b"new", 0o644, 0, 0),
        "mkdir": lambda: st.mkdir(ROOT, b"new", 0o755, 0, 0),
        "write": lambda: st.write_at(file, 2, b"xy"),
        "truncate shrink": lambda: st.truncate(file, 2),
        "truncate grow": lambda: st.truncate(file, 64),
        "update metadata": lambda: st.update_meta(file, lambda m: setattr(m, "mode", 0o600)),
        "rename": lambda: st.rename(ROOT, b"f", ROOT, b"g"),
        "unlink": lambda: st.unlink(ROOT, b"f"),
        "rmdir": lambda: st.rmdir(ROOT, b"d"),
    }
    return calls[op]


@pytest.mark.parametrize("op, multi_exec, lua", MUTATION_COSTS)
def test_mutation_round_trips(redis_url, namespace, mode, op, multi_exec, lua):
    async def scenario():
        async with open_store(redis_url, namespace, mode) as (st, metrics):
            file, _ = await st.create_file(ROOT, b"f", 0o644, 0, 0)
            await st.write_at(file, 0, b"abcdef")
            await st.mkdir(ROOT, b"d", 0o755, 0, 0)
            _, rt = await _measure(st, metrics, _mutation(st, op, file))
            assert rt == (multi_exec, lua)[COST_COLUMN[mode]]

    run(scenario())


def test_rename_directory_keeps_child_inode_and_addressing(redis_url, namespace, mode):
    async def scenario():
        async with open_store(redis_url, namespace, mode) as (st, _):
            d, _ = await st.mkdir(ROOT, b"d", 0o755, 0, 0)
            child, _ = await st.create_file(d, b"f", 0o644, 0, 0)
            await st.rename(ROOT, b"d", ROOT, b"e")
            moved = Ref(ROOT.id, b"e", d.id)
            rows = await st.list_children(moved)
            assert [(c.name, c.id) for c in rows] == [(b"f", child.id)]
            assert (await st.stat(child)).id == child.id

    run(scenario())


def test_rename_cost_does_not_grow_with_children(redis_url, namespace, mode):
    async def scenario():
        async with open_store(redis_url, namespace, mode) as (st, metrics):
            d, _ = await st.mkdir(ROOT, b"d", 0o755, 0, 0)
            for i in range(50):
                await st.create_file(d, f"c{i}".encode(), 0o644, 0, 0)
            _, rt = await _measure(st, metrics, lambda: st.rename(ROOT, b"d", ROOT, b"x"))
            assert rt == {"no": 2, "yes": 1}[mode]

    run(scenario())


REPLACE_TABLE = [
    # (src kind, dst kind, replace, want error or None)
    ("file", "file", True, None),
    ("dir", "dir", True, None),
    ("dir", "file", True, NotDir),
    ("file", "dir", True, IsDir),
    ("file", "file", False, Exists),
]


@pytest.mark.parametrize("src, dst, replace, want", REPLACE_TABLE)
def test_rename_replace_rules(redis_url, namespace, mode, src, dst, replace, want):
    async def scenario():
        async with open_store(redis_url, namespace, mode) as (st, _):
            await _make(st, src, b"src")
            await _make(st, dst, b"dst")
            if want is None:
                await st.rename(ROOT, b"src", ROOT, b"dst", replace=replace)
                assert [c.name for c in await st.list_children(ROOT)] == [b"dst"]
            else:
                with pytest.raises(want):
                    await st.rename(ROOT, b"src", ROOT, b"dst", replace=replace)

    run(scenario())


def test_rename_onto_nonempty_directory_is_refused(redis_url, namespace, mode):
    async def scenario():
        async with open_store(redis_url, namespace, mode) as (st, _):
            await _make(st, "dir", b"src")
            dst, _ = await st.mkdir(ROOT, b"dst", 0o755, 0, 0)
            await st.create_file(dst, b"x", 0o644, 0, 0)
            with pytest.raises(NotEmpty):
                await st.rename(ROOT, b"src", ROOT, b"dst")

    run(scenario())


def test_rename_of_a_missing_name_is_not_found(redis_url, namespace, mode):
    async def scenario():
        async with open_store(redis_url, namespace, mode) as (st, _):
            with pytest.raises(NotFound):
                await st.rename(ROOT, b"missing", ROOT, b"x")

    run(scenario())


def test_rmdir_and_unlink_refuse_wrong_kinds(redis_url, namespace, mode):
    async def scenario():
        async with open_store(redis_url, namespace, mode) as (st, _):
            d, _ = await st.mkdir(ROOT, b"d", 0o755, 0, 0)
            await st.create_file(ROOT, b"f", 0o644, 0, 0)
            await st.create_file(d, b"inner", 0o644, 0, 0)
            with pytest.raises(IsDir):
                await st.unlink(ROOT, b"d")
            with pytest.raises(NotDir):
                await st.rmdir(ROOT, b"f")
            with pytest.raises(NotEmpty):
                await st.rmdir(ROOT, b"d")
            with pytest.raises(NotFound):
                await st.unlink(ROOT, b"missing")
            with pytest.raises(NotFound):
                await st.rmdir(ROOT, b"missing")

    run(scenario())


def test_write_after_unlink_does_not_recreate_content(redis_url, namespace, mode):
    async def scenario():
        async with open_store(redis_url, namespace, mode) as (st, _):
            ref, _ = await st.create_file(ROOT, b"f", 0o644, 0, 0)
            await st.unlink(ROOT, b"f")
            with pytest.raises(NotFound):
                await st.write_at(ref, 0, b"late")
            with pytest.raises(NotFound):
                await st.truncate(ref, 4)
            raw = aioredis.from_url(redis_url)
            try:
                assert await raw.exists(f"kvfs/{{{namespace}}}/1/f/data") == 0
            finally:
                await raw.aclose()

    run(scenario())


def test_truncate_grows_with_zero_fill_and_shrinks(redis_url, namespace, mode):
    async def scenario():
        async with open_store(redis_url, namespace, mode) as (st, _):
            ref, _ = await st.create_file(ROOT, b"f", 0o644, 0, 0)
            await st.write_at(ref, 0, b"abcdef")
            assert (await st.truncate(ref, 3)).size == 3
            assert await st.get_data(ref) == b"abc"
            assert (await st.truncate(ref, 6)).size == 6
            assert await st.get_data(ref) == b"abc\x00\x00\x00"
            assert (await st.truncate(ref, 0)).size == 0

    run(scenario())


def test_symlink_stores_target_as_content(redis_url, namespace, mode):
    async def scenario():
        async with open_store(redis_url, namespace, mode) as (st, _):
            ref, meta = await st.symlink(ROOT, b"l", b"/etc/hosts", 0, 0)
            assert meta.type == "link" and meta.mode == 0o777
            assert await st.get_data(ref) == b"/etc/hosts"
            assert (await st.stat(ref)).size == len(b"/etc/hosts")

    run(scenario())


def test_update_meta_stamps_ctime_and_carries_the_size(redis_url, namespace, mode):
    async def scenario():
        async with open_store(redis_url, namespace, mode) as (st, _):
            ref, _ = await st.create_file(ROOT, b"f", 0o600, 0, 0)
            await st.write_at(ref, 0, b"1234")
            meta = await st.update_meta(ref, lambda m: setattr(m, "mode", 0o640))
            assert (meta.mode, meta.size) == (0o640, 4) and meta.ctime > 0

    run(scenario())


def test_every_mutation_publishes_its_address(redis_url, namespace, mode):
    async def scenario():
        async with open_store(redis_url, namespace, mode) as (st, _):
            pubsub = await st.subscribe()
            await pubsub.get_message(timeout=0.5)
            await st.create_file(ROOT, b"a", 0o644, 0, 0)
            await st.rename(ROOT, b"a", ROOT, b"b")
            await st.unlink(ROOT, b"b")
            try:
                assert await _published(pubsub) == [b"1/a", b"1/a", b"1/b", b"1/b"]
            finally:
                await pubsub.aclose()

    run(scenario())


def test_both_modes_write_the_same_keys(redis_url):
    """The same sequence in each mode leaves the same keys and values, timestamps aside."""

    async def scenario():
        dumps = []
        for mode in ("no", "yes"):
            ns = "t" + uuid.uuid4().hex[:12]
            async with open_store(redis_url, ns, mode) as (st, _):
                await _exercise(st)
            dumps.append(await _dump(redis_url, ns))
        assert dumps[0] == dumps[1] and dumps[0]

    run(scenario())


async def _exercise(st: Store) -> None:
    d, _ = await st.mkdir(ROOT, b"d", 0o755, 0, 0)
    e, _ = await st.mkdir(d, b"e", 0o700, 0, 0)
    f, _ = await st.create_file(e, b"f", 0o644, 0, 0)
    await st.write_at(f, 0, b"hello")
    await st.write_at(f, 8, b"world")
    await st.truncate(f, 10)
    await st.symlink(ROOT, b"l", b"d/e/f", 0, 0)
    await st.create_file(ROOT, b"gone", 0o644, 0, 0)
    await st.unlink(ROOT, b"gone")
    await st.mkdir(ROOT, b"empty", 0o755, 0, 0)
    await st.rmdir(ROOT, b"empty")
    await st.rename(ROOT, b"d", ROOT, b"moved")
    created, _ = await st.create_file(ROOT, b"x", 0o644, 0, 0)
    await st.write_at(created, 0, b"x")
    created, _ = await st.create_file(ROOT, b"y", 0o644, 0, 0)
    await st.write_at(created, 0, b"y")
    await st.rename(ROOT, b"x", ROOT, b"y", replace=True)


async def _dump(url: str, ns: str) -> dict:
    """Every key of a namespace with its value, the namespace and timestamps replaced."""
    raw = aioredis.from_url(url)
    try:
        dump = {}
        async for key in raw.scan_iter(match=f"kvfs/{{{ns}}}/*"):
            dump[key.replace(ns.encode(), b"NS")] = await _value(raw, key)
        return dump
    finally:
        await raw.aclose()


async def _value(raw, key: bytes):
    kind = await raw.type(key)
    if kind == b"hash":
        return await raw.hgetall(key)
    value = await raw.get(key)
    if not key.endswith(b"/meta"):
        return value
    doc = json.loads(value)
    return {k: v for k, v in doc.items() if not k.endswith("time")}


def test_auto_uses_scripts_when_the_server_allows_them(redis_url, namespace):
    async def scenario():
        pool = make_pool(redis_url, Metrics())
        try:
            assert isinstance(await make_store(pool, namespace, sss_mode="auto"), LuaStore)
        finally:
            await pool.disconnect()

    run(scenario())


@contextlib.asynccontextmanager
async def _user_without_scripts(redis_url: str, namespace: str):
    """A pool for an ACL user that may use everything except scripting."""
    user = "kvfs-" + namespace
    admin = aioredis.from_url(redis_url)
    await admin.execute_command("ACL", "SETUSER", user, "on", "nopass", "~*", "&*", "+@all", "-@scripting")
    pool = make_pool(redis_url.replace("://", f"://{user}:x@", 1), Metrics())
    try:
        yield pool
    finally:
        await pool.disconnect()
        await admin.execute_command("ACL", "DELUSER", user)
        await admin.aclose()


def test_auto_falls_back_to_transactions_without_scripting(redis_url, namespace):
    async def scenario():
        async with _user_without_scripts(redis_url, namespace) as pool:
            st = await make_store(pool, namespace, sss_mode="auto")
            assert isinstance(st, MultiExecStore) and not st.uses_scripts
            await st.bootstrap_root(0, 0)
            ref, _ = await st.create_file(ROOT, b"f", 0o644, 0, 0)
            assert ref.id == 2

    run(scenario())


def test_yes_refuses_to_start_without_scripting(redis_url, namespace):
    async def scenario():
        async with _user_without_scripts(redis_url, namespace) as pool:
            with pytest.raises(ScriptsRefused, match="--sss-mode=auto"):
                await make_store(pool, namespace, sss_mode="yes")

    run(scenario())


def test_no_never_uses_scripts_even_when_the_server_allows_them(redis_url, namespace):
    async def scenario():
        pool = make_pool(redis_url, Metrics())
        try:
            st = await make_store(pool, namespace, sss_mode="no")
            assert isinstance(st, MultiExecStore) and not st.uses_scripts
        finally:
            await pool.disconnect()

    run(scenario())


def test_no_does_not_probe_the_server(redis_url, namespace):
    async def scenario():
        metrics = Metrics()
        pool = make_pool(redis_url, metrics)
        try:
            await make_store(pool, namespace, sss_mode="no")
            assert metrics.round_trips == 0
        finally:
            await pool.disconnect()

    run(scenario())


@pytest.mark.parametrize("sss_mode, want", [("yes", LuaStore), ("auto", LuaStore), ("no", MultiExecStore)])
def test_sss_mode_values_pick_the_store(redis_url, namespace, sss_mode, want):
    async def scenario():
        pool = make_pool(redis_url, Metrics())
        try:
            assert type(await make_store(pool, namespace, sss_mode=sss_mode)) is want
        finally:
            await pool.disconnect()

    run(scenario())


REFUSED_OPENINGS = [
    # (namespace, level, sss_mode, error)
    ("ok", "dir", "auto", NotImplementedError),
    ("ok", "file", "yes", NotImplementedError),
    ("ok", "directory", "auto", ValueError),
    ("bad/ns", "namespace", "auto", ValueError),
    ("ok", "namespace", "lua", ValueError),
    ("ok", "namespace", "true", ValueError),
]


@pytest.mark.parametrize("ns, level, sss_mode, want", REFUSED_OPENINGS)
def test_bad_openings_are_refused_before_any_server_call(redis_url, ns, level, sss_mode, want):
    async def scenario():
        metrics = Metrics()
        pool = make_pool(redis_url, metrics)
        try:
            with pytest.raises(want):
                await make_store(pool, ns, level, sss_mode)
            assert metrics.round_trips == 0
        finally:
            await pool.disconnect()

    run(scenario())


def test_namespace_written_at_another_level_is_refused(redis_url, namespace, mode):
    async def scenario():
        raw = aioredis.from_url(redis_url)
        try:
            await raw.set(f"kvfs/{{{namespace}/0}}/./counter", 1)
        finally:
            await raw.aclose()
        with pytest.raises(Placement):
            async with open_store(redis_url, namespace, mode):
                pass

    run(scenario())


def test_busy_is_raised_when_keys_keep_changing(redis_url, namespace, monkeypatch):
    async def scenario():
        async with open_store(redis_url, namespace) as (st, _):
            monkeypatch.setattr("kvfs.store.store.MAX_ATTEMPTS", 2)

            async def always_conflicts(_session):
                return _CONFLICT

            with pytest.raises(Busy):
                await st._optimistic(always_conflicts)

    run(scenario())


def test_store_errors_share_one_base_class():
    for err in (NotFound, Exists, NotEmpty, NotDir, IsDir, Busy, Placement):
        assert issubclass(err, KvfsError)


async def _make(st: Store, kind, name: bytes):
    if kind == "dir":
        return await st.mkdir(ROOT, name, 0o755, 0, 0)
    return await st.create_file(ROOT, name, 0o644, 0, 0)


async def _published(pubsub) -> list[bytes]:
    got = []
    while True:
        message = await pubsub.get_message(timeout=0.2)
        if message is None:
            return got
        if message.get("type") == "message":
            got.append(message["data"])
