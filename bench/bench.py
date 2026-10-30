"""Store-level benchmark, Python side. Same operations, sizes and names as bench/go/harness_test.go.txt.

For each directory size N in (10, 1000, 10000) and each operation it reports:
  rt       round trips sent to the server per call (counted by the connection, not estimated)
  cmds     Redis commands executed per call (INFO commandstats delta, INFO itself excluded)
  med_us   median wall time per call in microseconds

Run: python3 bench/bench.py REDIS_URL ITERS [SSS] > bench/results/python-lua.jsonl
SSS is a --sss-mode value (yes, no or auto; default auto). Rows are named python-lua or
python-multi-exec, by the store that ran.
"""

import asyncio
import json
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import redis.asyncio as aioredis  # noqa: E402

from kvfs.store import ROOT, Ref, Store, make_pool, open_store  # noqa: E402
from kvfs.store.wire import Metrics  # noqa: E402

SIZES = (10, 1000, 10000)


async def commands_total(raw: aioredis.Redis) -> int:
    """Sum of calls across commandstats, minus INFO's own calls."""
    info = await raw.info("commandstats")
    total = 0
    for name, stats in info.items():
        if name == "cmdstat_info":
            continue
        total += int(stats["calls"])
    return total


def impl_name(st: Store) -> str:
    return "python-lua" if st.uses_scripts else "python-multi-exec"


class Recorder:
    def __init__(self, raw: aioredis.Redis, metrics: Metrics, impl: str, size: int, out) -> None:
        self.raw, self.metrics, self.impl, self.size, self.out = raw, metrics, impl, size, out

    async def measure(self, op: str, iters: int, setup, run) -> None:
        durations, rts, cmds = [], 0, 0
        for i in range(iters):
            await setup(i)
            before_rt = self.metrics.round_trips
            before_cmd = await commands_total(self.raw)
            start = time.perf_counter()
            await run(i)
            durations.append(time.perf_counter() - start)
            rts += self.metrics.round_trips - before_rt
            cmds += await commands_total(self.raw) - before_cmd
        row = {
            "impl": self.impl, "n": self.size, "op": op,
            "rt": rts / iters, "cmds": cmds / iters,
            "median_us": statistics.median(durations) * 1e6, "iters": iters,
        }
        self.out.write(json.dumps(row) + "\n")
        self.out.flush()


async def noop(_i):
    return None


async def bench_size(url: str, sss: str, size: int, iters: int, out) -> None:
    metrics = Metrics()
    pool = make_pool(url, metrics)
    ns = f"bench-{size}"
    raw = aioredis.from_url(url)
    try:
        st = await open_store(pool, ns, sss_mode=sss)
        await st.bootstrap_root(1000, 1000)
        await populate(st, size)
        d = await directory(st)
        rec = Recorder(raw, metrics, impl_name(st), size, out)
        await run_ops(st, rec, d, iters)
    finally:
        await raw.aclose()
        await pool.disconnect()


async def directory(st: Store) -> Ref:
    rows = await st.list_children(ROOT)
    child = next(c for c in rows if c.name == b"d")
    return Ref(ROOT.id, b"d", child.id)


async def populate(st: Store, size: int) -> None:
    d, _ = await st.mkdir(ROOT, b"d", 0o755, 1000, 1000)
    for i in range(size):
        await st.create_file(d, b"f%05d" % i, 0o644, 1000, 1000)
    x, _ = await st.create_file(d, b"f00000x", 0o644, 1000, 1000)
    await st.write_at(x, 0, bytes(64))


async def _inode(st: Store, d: Ref, name: bytes) -> int:
    meta = await st.stat(Ref(d.id, name, 0))
    return meta.id


async def run_ops(st: Store, rec: Recorder, d: Ref, iters: int) -> None:
    x = Ref(d.id, b"f00000x", await _inode(st, d, b"f00000x"))
    root = ROOT

    async def stat_op(_i):
        await st.stat(Ref(d.id, b"f00000x", x.id))

    await rec.measure("stat", iters, noop, stat_op)
    await rec.measure("readdir", iters, noop, lambda _i: st.list_children(d))
    await rec.measure("readdirplus", iters, noop, lambda _i: st.list_with_attrs(d))

    await rec.measure("create", iters, noop, lambda i: st.create_file(d, b"c%d" % i, 0o644, 1000, 1000))
    await rec.measure("mkdir", iters, noop, lambda i: st.mkdir(d, b"m%d" % i, 0o755, 1000, 1000))

    await rec.measure("write64", iters, noop, lambda _i: st.write_at(x, 0, bytes(64)))
    await rec.measure("read64", iters, noop, lambda _i: st.read_at(x, 0, 64))

    async def rename_file(i):
        src, dst = (b"f00000x", b"f00000y") if i % 2 == 0 else (b"f00000y", b"f00000x")
        await st.rename(d, src, d, dst)

    await rec.measure("rename_file", iters, noop, rename_file)

    async def create_for_unlink(i):
        await st.create_file(d, b"u%d" % i, 0o644, 1000, 1000)

    await rec.measure("unlink", iters, create_for_unlink, lambda i: st.unlink(d, b"u%d" % i))

    async def mkdir_for_rmdir(i):
        await st.mkdir(d, b"e%d" % i, 0o755, 1000, 1000)

    await rec.measure("rmdir_empty", iters, mkdir_for_rmdir, lambda i: st.rmdir(d, b"e%d" % i))

    await rec.measure("truncate", iters, noop, lambda _i: st.truncate(x, 128))

    async def rename_dir(i):
        src, dst = (b"d", b"d2") if i % 2 == 0 else (b"d2", b"d")
        await st.rename(root, src, root, dst)

    await rec.measure("rename_dir", iters, noop, rename_dir)


async def main(url: str, iters: int, sss: str) -> None:
    for size in SIZES:
        print(f"# size {size}", file=sys.stderr)
        await bench_size(url, sss, size, iters, sys.stdout)


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1], int(sys.argv[2]), sys.argv[3] if len(sys.argv) > 3 else "auto"))
