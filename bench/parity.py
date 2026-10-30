"""Runs the fixed parity sequence through the Python store and prints one line per step.

The same steps, in the same order, are in bench/go/harness_test.go.txt (TestHarnessParity).
Both print "<step> <outcome>". The outcome names match the Go errName table.
Run: python3 bench/parity.py REDIS_URL NAMESPACE [SSS]   (SSS: a --sss-mode value, default auto)
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from kvfs.store import Exists, IsDir, NotDir, NotEmpty, NotFound, ROOT, Ref, Store, make_pool, open_store  # noqa: E402
from kvfs.store.errors import KvfsError  # noqa: E402
from kvfs.store.wire import Metrics  # noqa: E402

OUTCOMES = [(NotFound, "NotFound"), (Exists, "Exists"), (NotEmpty, "NotEmpty"), (NotDir, "NotDir"), (IsDir, "IsDir")]


def outcome(err) -> str:
    if err is None:
        return "ok"
    for cls, name in OUTCOMES:
        if isinstance(err, cls):
            return name
    return f"other:{err}" if isinstance(err, KvfsError) else f"other:{err}"


async def step(label: str, call) -> object:
    """Runs one step, prints its outcome, and returns the result (None on error)."""
    try:
        result = await call()
        print(f"{label:<28} {outcome(None)}")
        return result
    except Exception as err:  # noqa: BLE001 - the outcome is the point of the step
        print(f"{label:<28} {outcome(err)}")
        return None


async def run(url: str, ns: str, sss: str) -> None:
    pool = make_pool(url, Metrics())
    try:
        st = await open_store(pool, ns, sss_mode=sss)
        print(f"# server-side scripts {'on' if st.uses_scripts else 'off'}", file=sys.stderr)
        await st.bootstrap_root(1000, 1000)
        await sequence(st)
    finally:
        await pool.disconnect()


async def sequence(st: Store) -> None:
    a, _ = await step("create a", lambda: st.create_file(ROOT, b"a", 0o644, 1000, 1000)) or (None, None)
    await step("write a", lambda: st.write_at(a, 0, b"hello"))
    d, _ = await step("mkdir d", lambda: st.mkdir(ROOT, b"d", 0o755, 1000, 1000)) or (None, None)
    f, _ = await step("create d/f", lambda: st.create_file(d, b"f", 0o644, 1000, 1000)) or (None, None)
    await step("symlink l->a", lambda: st.symlink(ROOT, b"l", b"a", 1000, 1000))
    await step("rename a->b", lambda: st.rename(ROOT, b"a", ROOT, b"b"))
    await step("rename d->e", lambda: st.rename(ROOT, b"d", ROOT, b"e"))
    d = Ref(ROOT.id, b"e", d.id)  # the directory's address changed with its name
    ef = Ref(d.id, b"f", f.id)
    await step("truncate e/f 5", lambda: st.truncate(ef, 5))
    await step("write e/f at 5", lambda: st.write_at(ef, 5, b"!!"))
    await step("unlink l", lambda: st.unlink(ROOT, b"l"))
    await step("unlink b", lambda: st.unlink(ROOT, b"b"))
    await step("rmdir e (not empty)", lambda: st.rmdir(ROOT, b"e"))
    await step("create e/g", lambda: st.create_file(d, b"g", 0o600, 1000, 1000))
    await step("unlink e/g", lambda: st.unlink(d, b"g"))
    await step("rmdir e (empty)", lambda: st.rmdir(ROOT, b"e"))
    x, _ = await step("mkdir x", lambda: st.mkdir(ROOT, b"x", 0o755, 1000, 1000)) or (None, None)
    await step("create x/y", lambda: st.create_file(x, b"y", 0o644, 1000, 1000))
    await step("rename x->z (dir)", lambda: st.rename(ROOT, b"x", ROOT, b"z"))
    await step("unlink z/y", lambda: st.unlink(Ref(ROOT.id, b"z", x.id), b"y"))
    await step("rmdir z", lambda: st.rmdir(ROOT, b"z"))
    await step("create p", lambda: st.create_file(ROOT, b"p", 0o644, 1000, 1000))
    await step("create q", lambda: st.create_file(ROOT, b"q", 0o644, 1000, 1000))
    await step("rename-noreplace p->q", lambda: st.rename(ROOT, b"p", ROOT, b"q", replace=False))
    await step("rename p->q (replace)", lambda: st.rename(ROOT, b"p", ROOT, b"q"))


if __name__ == "__main__":
    asyncio.run(run(sys.argv[1], sys.argv[2], sys.argv[3] if len(sys.argv) > 3 else "auto"))
