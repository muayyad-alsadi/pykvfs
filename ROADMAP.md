# Roadmap

[`SPEC.md`](SPEC.md) is the design of the Go `kvfs`. pykvfs implements its `namespace` level only, as the subset note at the top of `SPEC.md` says. This file lists what is not done, with the evidence where there is some. The Go implementation's roadmap is in its own repository.

## Not done

### 1. The `dir` and `file` sharding levels

`-shard-level=dir` and `-shard-level=file` raise `NotImplementedError` before any key is read. The Go store implements both: the link protocol (claim, prepare, settle), the repair pass and `-repair`.

- Done when: the Go store's `dir` and `file` tests have Python counterparts, and `bench/parity.py` matches the Go harness at both levels.

### 2. Writable opens keep the kernel page cache

pykvfs sets no `FOPEN_DIRECT_IO` flag. The Go mount bypasses the cache on writable opens, so each write the caller makes reaches Redis as one request. pykvfs does not, and the subset note in `SPEC.md` says so.

- Done when: a writable open bypasses the cache as in Go, and the Go repository's `scripts/coherence.sh` passes in both directions between a pykvfs mount and a Go mount.

### 3. Python spends its time in the request path, not in Redis

With scripts on, pykvfs sends the same Redis commands as Go, and its extract is still about 2.5 times slower. On the ansible tarball (8,637 entries, `tar -xf` into the mount, median of 3 runs on 2026-10-30):

| | extract | daemon CPU during extract |
|---|---:|---:|
| tmpfs (baseline) | 0.17 s | not applicable |
| Go | 23.2 s | 17.6 s |
| pykvfs, `--sss-mode=yes` | 57.7 s | 49.9 s |
| pykvfs, `--sss-mode=no` | 65.2 s | 54.7 s |

Redis round trips are not the cost. The two Python rows differ by about 13%, while a mutation's round trips differ by up to a factor of three. The next step is the Python request path: pyfuse3 dispatch, redis-py, and the coroutines in `kvfs/fs.py`.

- Done when: the pykvfs extract is within 1.5 times the Go extract, measured the same way.

### 4. Extra reads against Go for the same workload

On the same tarball, one pykvfs run sent about 17,500 `EVAL` and 51,000 `STRLEN` commands, against 16,300 and 42,700 for one Go run (`INFO commandstats` deltas). The Redis work per mutation is the same. The difference is probably in which FUSE requests pyfuse3 sends after each create, such as extra lookups or getattr calls. That is not checked.

- Done when: the difference is explained, with the FUSE request counts from both implementations.

### 5. Authentication and TLS are not tested

The connection is a URL. Credentials in the URL work: the tests run against an ACL user. TLS (`rediss://`) is not tested, and there is no flag for either.

- Done when: a mount against a server that requires a password and TLS works, and the test suite covers both.

### 6. Multi-host and crash behavior are not tested

There has been no run with mounts on two machines, and no test that kills the mount during a workload. The Go roadmap's items 2 and 3 describe the same gaps, and the SQLite limit in its item 6 applies here too: this implementation has no byte-range locks.

- Done when: the two checks the Go roadmap asks for pass against pykvfs as well.

### 7. No CI

The repository has an `origin` on GitHub and no workflow file. The tests start their own `redis-server`, so a runner needs `redis-server` or `valkey-server` on its path, and `pyfuse3` needs the libfuse 3 headers to install.

- Done when: a workflow runs `python3 -m pytest -q tests` on each push.

## Known limits the spec accepts

- Namespace level only (item 1).
- Without scripts (`--sss-mode=no`), a mutation costs two or three round trips, and a transaction that loses to a concurrent writer is retried. After 64 attempts the operation fails with `Busy`, which the mount reports as `EBUSY`.
- The `auto` probe runs once, at startup. A server that changes its scripting setting while the mount runs keeps the path that was chosen. Restart the mount to change it.
- The Go store's unlink race (its roadmap, item 8) does not apply here. At `namespace` level unlink is one script.

## Verified

Run on 2026-10-30 with Python 3.14, redis-py 5.2.1, pyfuse3 3.4.2, Valkey 9.0.6 and Fedora.

| What | How | Result |
|---|---|---|
| Unit and store tests | `python3 -m pytest -q tests`, 140 tests; every store test runs with scripts on and off | pass |
| Scripts match Go byte for byte | one-off comparison of the seven constants in `kvfs/store/lua.py` with `internal/store` | identical |
| Same outcomes and keys as Go | `bench/parity.py` (24 steps) against the Go harness, with `yes`, `no` and `auto` | same outcomes, same normalized key dump |
| Startup and refusal | `kvfs` with `auto` and `no` mounted; `yes` against a user without `@scripting` exits with status 2 | as described |
| Filesystem benchmark | ansible tarball, tmpfs baseline, Go and pykvfs, median of 3 runs; every run's manifest matched | item 3 above |
| Store benchmark | `bench/bench.py` at store level, Go and pykvfs both paths | table in `README.md` |
