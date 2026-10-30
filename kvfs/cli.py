"""Command line: `kvfs MOUNTPOINT`. Wires the store, the FUSE layer and the invalidation task."""

from __future__ import annotations

import argparse
import asyncio
import os
import sys

import pyfuse3
import pyfuse3.asyncio

from .fs import Operations, watch_invalidations
from .store import SSS_MODES, make_pool, open_store
from .store import keys
from .store.errors import KvfsError
from .store.modes import AUTO
from .store.wire import Metrics


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="kvfs", description="Redis/Valkey-backed FUSE filesystem (namespace sharding).")
    ap.add_argument("mountpoint")
    ap.add_argument("-redis", default="redis://127.0.0.1:6379/0", help="single Redis/Valkey server URL")
    ap.add_argument("-ns", default="default", help="namespace, [A-Za-z0-9-]+")
    ap.add_argument("-shard-level", default=keys.NAMESPACE, help="only 'namespace' is implemented")
    ap.add_argument(
        "-sss-mode",
        "--sss-mode",
        dest="sss_mode",
        default=AUTO,
        choices=SSS_MODES,
        help="does the server support server-side scripts (sss)? "
        "yes: use them, and exit if the server refuses. "
        "no: never use them. "
        "auto: use them if the server allows, otherwise run without them (default)",
    )
    ap.add_argument("-allow-other", action="store_true", help="let other users access the mount")
    return ap


def _fuse_options(allow_other: bool) -> set[str]:
    options = set(pyfuse3.default_options)
    options.add("fsname=kvfs")
    options.add("default_permissions")
    if allow_other:
        options.add("allow_other")
    return options


async def _serve(args: argparse.Namespace) -> None:
    metrics = Metrics()
    pool = make_pool(args.redis, metrics)
    try:
        store = await open_store(pool, args.ns, args.shard_level, args.sss_mode)
        print(f"kvfs: server-side scripts {'on' if store.uses_scripts else 'off'}", file=sys.stderr)
        await store.bootstrap_root(os.getuid(), os.getgid())
        ops = Operations(store)
        pyfuse3.init(ops, args.mountpoint, _fuse_options(args.allow_other))
        watcher = asyncio.create_task(watch_invalidations(store, ops))
        try:
            await pyfuse3.main()
        finally:
            watcher.cancel()
            await asyncio.gather(watcher, return_exceptions=True)
            pyfuse3.close()
    finally:
        await pool.disconnect()


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    pyfuse3.asyncio.enable()
    try:
        asyncio.run(_serve(args))
    except (NotImplementedError, ValueError, KvfsError) as err:
        print(f"kvfs: {err}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
