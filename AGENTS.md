# AGENTS.md

Working notes for agents in this repo. `SPEC.md` holds the design (a copy of the Go
implementation's contract, with the subset note at the top); this file holds how the code is written.

`pykvfs` is a Redis/Valkey-backed FUSE3 filesystem in Python (pyfuse3 + redis-py asyncio). It is
wire-compatible with the Go `kvfs` at `namespace` sharding: both write the same keys. `kvfs/store`
owns every Redis call and the key builder, `kvfs/fs.py` owns FUSE nodes and errno mapping, and
`kvfs/cli.py` owns flags and wiring. Three layers on purpose; do not add a fourth.

## Commands

```bash
python3 -m pytest -q tests                  # starts its own redis-server unless KVFS_TEST_REDIS is set
python3 -m compileall -q kvfs tests bench   # syntax, must print nothing
```

Benchmark and parity runs need the Go harness (see README.md, "Reproducing"). They are not
part of `pytest` because they need a Go toolchain and a running server.

## Errors

- Raise store errors from `kvfs/store/errors.py`; never log an error and raise it too.
- Wrap with context: `raise NotFound(name) from err`, or re-raise with the operation named.
- Match with `except KvfsError` or `isinstance`, never by comparing message strings.
- `kvfs/store` owns the error classes; `kvfs/fs.py` maps them to errno in the single `ERRNO` table.
- No bare `except:`. Catch the narrowest type. `OSError` from `pyfuse3.invalidate_*` is the only catch
  that is a correct "already gone" signal, and it lives in `_drop_quietly`.
- `assert` is for tests only. Startup checks raise a real exception.

## Functions

- Guard clauses and early returns; no `else` after a `return`.
- One level of abstraction per function: it decides, or it does I/O, not both.
- Keep functions under roughly 40 lines and nesting under four levels.
- If a block needs a comment explaining *what* it does, extract it and use the comment as the name.
- A boolean parameter that flips behavior is two functions wearing one name. `rename(..., replace=)`
  is the one accepted case: it mirrors `rename(2)` and the store is the only caller of the flag.
- `match` or a dict table over a closed set of cases, not an if-chain repeating one comparison.

## Types and seams

- `Store` (`store.py`) is an ABC because two classes share it, `LuaStore` and `MultiExecStore`,
  and they share its reads and its watched update. Consumers (`fs.py`, `cli.py`) take `Store`.
  Declare a smallest `Protocol` at a consumer only when that consumer needs a subset a test double
  or a third class would have to satisfy, and put it in the consumer's module.
- No ABC with one implementation and one caller. No `XxxManager`/`XxxService` that forwards calls.
- Dependencies arrive through the constructor. No module-level mutable state.
- Only `kvfs/store/keys.py` concatenates keys. Everything else passes a `Ref` or calls the builder.
- No Redis call outside `kvfs/store/`. `kvfs/fs.py` and `kvfs/cli.py` never import `redis`.

## Concurrency (asyncio)

- Every task has an owner and a shutdown path. `cli._serve` cancels the invalidation watcher and
  awaits it before `pyfuse3.close()`.
- Kernel invalidation (`pyfuse3.invalidate_*`) blocks on `/dev/fuse` and can wait for a request this
  loop is serving. It runs in `asyncio.to_thread`, never on the loop. Calling it inline deadlocks
  the mount on the first directory operation.
- A WATCHed connection is held for the whole optimistic attempt (`kvfs/store/wire.py`, `Session`) and
  returned to the pool in `__aexit__`, including after `UNWATCH`.
- Do not hold a `Session` across an `await` on another store call. Sessions are short.

## Wire and transactions

- Two mutation paths, chosen by `--sss-mode` (`kvfs/store/modes.py`): `yes` and `no`, with `auto`
  picking one by probing the server. `Store` (`store.py`) holds what they share: keys, reads, the
  watched metadata update, pub/sub. `MultiExecStore` (`multi_exec.py`) commits with WATCH/MULTI/EXEC.
  `LuaStore` (`lua.py`) sends the Go store's scripts. A behavior change lands in both, or in `Store`.
  The user-facing name is `--sss-mode`; the code says `uses_scripts`. Keep the flag jargon-free.
- `lua.py` holds the Go scripts byte for byte, sent as `EVAL` with Go's key and argument order.
  Do not edit a script here; change it in Go and copy it, or the two stores stop being one protocol.
- A `None` EXEC reply means a watched key changed: retry through `Store._optimistic`, bounded by
  `MAX_ATTEMPTS`, then raise `Busy`. The metadata update is watched in both modes, as in Go.
- Store tests take the `mode` fixture (`sss-mode` no and yes), so each runs on both paths.
  Round-trip expectations live in one table (`MUTATION_COSTS`).
- Every round trip goes through `CountedConnection`. Tests assert round-trip counts through `Metrics`;
  do not estimate them.
- Decide before writing: a refused create, rename or unlink writes nothing and burns no inode number.

## Tests

- pytest, plain functions, and `asyncio.run` for async bodies. No `pytest-asyncio`.
- Real Redis: `tests/conftest.py` starts `redis-server` (or `valkey-server`). Fakes are not allowed for
  store behavior. Each test takes its own namespace (`namespace` fixture).
- `tmp_path` for paths, no `time.sleep` (use `get_message(timeout=)` for pub/sub).
- Assert the wire contract: exact key bytes (golden table in `test_keys.py`), the round trips an
  operation costs (`Metrics`), and the publish payloads.
- Anything that needs a real mount is not a unit test; it belongs in the README smoke checklist.

## Dependencies

Standard library first. Runtime: `pyfuse3`, `redis` (with `hiredis`). Test: `pytest`. Ask before
adding another.

## Release

PyPI never accepts a version twice, so a release is one version bump and one upload. The current
release is in `pyproject.toml`, and the published versions are at https://pypi.org/project/pykvfs/.

1. Bump `version` in `pyproject.toml` and `__version__` in `kvfs/__init__.py` together. Before 1.0, a
   new flag or a changed default is a minor bump.
2. Gates: `python3 -m pytest -q tests` passes and `python3 -m compileall -q kvfs tests bench` prints
   nothing. `bench/parity.py` in both `yes` and `no` matches the Go harness (README, Reproducing).
3. Build into a fresh directory: `python3 -m build --outdir /tmp/pykvfs-dist`. The `dist/` folder in
   a checkout holds old files; never upload from it.
4. Check the wheel: `python3 -m zipfile -l /tmp/pykvfs-dist/*.whl` lists `kvfs/` and nothing from
   `tests/`, `bench/` or `kvfs_min.py`.
5. Install the wheel in a clean virtualenv, run `kvfs --help`, and mount once against a server.
6. `python3 -m twine check /tmp/pykvfs-dist/*`, then `python3 -m twine upload /tmp/pykvfs-dist/*`.
   Credentials come from `~/.pypirc`; never print them.
7. Commit the version bump and tag it `vX.Y.Z`.

## Done means

- `python3 -m pytest -q tests` is green.
- New public names have docstrings; new behavior has a store-level test.
- No key string outside `kvfs/store/keys.py`; no Redis call outside `kvfs/store/`.
- If the store changed, rerun `bench/parity.py` against the Go harness: outcomes and the normalized key
  dump must both match.
- The round-trip table in `README.md` is regenerated when a store operation changes its cost.
