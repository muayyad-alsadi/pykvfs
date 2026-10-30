# pykvfs

Python implementation of [kvfs](https://github.com/muayyad-alsadi/kvfs): Key-Value backed networked shared filesystem using redis/valkey server.

A FUSE3 filesystem whose metadata and file contents live in Redis or Valkey. It is a Python
implementation of the key schema and operations of the Go `kvfs` (see `SPEC.md`), and it writes
the same keys, so a mount of either implementation reads the other's namespace.

```
pip install pykvfs
kvfs /mnt/kvfs -redis redis://127.0.0.1:6379/0 -ns default
```

Requirements: Linux with FUSE 3 (`fusermount3`), Python 3.10 or newer, and a Redis or Valkey server.
Building `pyfuse3` from source needs the libfuse 3 development headers (`fuse3-devel` on Fedora,
`libfuse3-dev` on Debian and Ubuntu). To work on this code, install it from a checkout with
`pip install -e '.[test]'`.

pykvfs is the Python implementation of the Go `kvfs` at `namespace` level. It writes the same keys, so
a namespace mounted by either one reads the other's files. The Go implementation also has the `dir`
and `file` sharding levels, a repair pass and `-repair`, and it always runs mutations as scripts.
The differences are in the subset table below and in [`ROADMAP.md`](https://github.com/muayyad-alsadi/pykvfs/blob/main/ROADMAP.md).

Most users need no other flag. `--sss-mode` answers one question: does the Redis/Valkey server
support server-side scripts (sss), the Lua scripts the Go version uses?

| `--sss-mode` | what kvfs does |
|---|---|
| `auto` (default) | checks the server, uses scripts if it allows them, and otherwise runs without them |
| `yes` | uses scripts; exits with an error if the server does not allow them |
| `no` | never uses scripts, even when the server allows them |

The startup line says which path is in use: `kvfs: server-side scripts on` or `... off`. Both paths
write the same keys, so mounts that use different settings, and Go mounts, can share one namespace.
Without scripts, each mutation is a `WATCH`/`MULTI`/`EXEC` transaction and costs more round trips
(see the table below).

## Subset of SPEC.md

`SPEC.md` is the Go project's design contract, copied here. pykvfs implements a **subset** of it:

| | in pykvfs | |
|---|---|---|
| key grammar, escaping, root `0/./`, counter, pub/sub, listing hash, JSON metadata | yes | byte-identical at rest (checked against Go, see Parity) |
| `-shard-level=namespace` | yes | the only level |
| `-shard-level=dir`, `-shard-level=file` | no | raise `NotImplementedError`, before any key is read |
| single Redis/Valkey server | yes | no cluster support |
| atomic mutations | Go's Lua scripts (`--sss-mode=yes`), or `WATCH` / `MULTI` / `EXEC` (`--sss-mode=no`); `auto` picks one | the metadata update is a watched transaction in both paths, as in Go |
| `RENAME_EXCHANGE` | refused with `EINVAL` | same as Go |
| dir/file link protocol, repair pass, `-repair` | no | Go only |
| kernel page cache on writable opens (`FOPEN_DIRECT_IO`) | no, the cache stays on | Go bypasses it |

A namespace written by Go at `namespace` level mounts unchanged here, and the reverse holds.

## Round trips and complexity per operation

`O(1)` means a fixed number of Redis commands no matter how many entries the directory holds.
Round trips are counted on the connection (`kvfs/store/wire.py`), not estimated. Commands are the
`INFO commandstats` delta, so commands inside a Lua script count too.

The cost of a mutation is set by the protocol. With Lua, one script does the check and the
write in one round trip. With `WATCH`, the check has to be read before `MULTI` can be sent, so a
conditional write costs at least two round trips, and a create also needs an `INCR`. That is the
price of not using Lua. With scripts on, Python sends the same commands as Go for every mutation.

| operation | commands | round trips Go | Python with scripts (`yes`) | Python without (`no`) |
|---|---|---:|---:|---:|
| lookup / getattr (`stat`) | O(1) | 1 | 1 | 1 |
| readdir (names and types, `HGETALL`) | O(1) commands, O(N) bytes returned | 1 | 1 | 1 |
| readdirplus (names with attributes, as the kernel asks for them) | Go: O(N) commands, one `Stat` per entry. Python: O(N) commands, one pipelined `GET` + `STRLEN` per entry | N + 2 | 2 | 2 |
| create / mkdir / symlink | O(1) | 1 | 1 | 3 (`WATCH` read, `INCR`, `MULTI`/`EXEC`) |
| write (64 B, `SETRANGE`) | O(1), cost is the bytes written | 1 | 1 | 2 |
| read (`GETRANGE`) | O(1) | 1 | 1 | 1 |
| rename a file, same directory | O(1), no bytes moved | 1 | 1 | 2 |
| rename a directory with N children | O(1), children untouched (they are addressed by inode) | 1 | 1 | 2 |
| unlink | O(1) | 1 | 1 | 2 |
| rmdir (empty) | O(1), emptiness is `HLEN` | 1 | 1 | 2 |
| truncate (grow or shrink) | O(1) commands for growth, shrink reads the kept bytes | 3 | 3 | 2 (3 for a shrink) |
| metadata update (chmod, chown, utimes) | O(1) | 2 | 2 | 2 |

Truncate in Go and with scripts on is the script, then the watched metadata update that stamps
`mtime`. Without scripts, one transaction does both.

`readdirplus` is the one operation whose round trips and commands are both not O(1). In Go it
is O(N) round trips, because go-fuse calls `Lookup` once per entry (`fuse/bridge.go`,
`readDirMaybeLookup`). In Python it is O(1) round trips, two, but still O(N) commands: each child
needs its attributes, so the server still runs O(N) commands. Pipelining hides the round trips
and does nothing about the commands.

A rename of a directory costs the same at N = 10, 1 000 and 10 000 in both implementations. The
children of a directory are addressed by its inode, and the inode does not change.

Latency figures are for the machine this was measured on: loopback Valkey 9.0.6, Python 3.14 with
redis-py 5.2.1 and pyfuse3 3.4.2, Go with rueidis v1.0.76. Python's per-operation cost on
loopback is 0.5 to 0.8 ms for one round trip, Go's is 0.1 to 0.3 ms. Compare round trips and commands. Use the
latency only as a measure of this client stack.

### Benchmark table

Median of 20 runs per operation, store level (no FUSE mount in the loop), directory of N files.
Go is the store with the watched metadata update (truncate is 3 round trips).

| operation | N | round trips Go | round trips Py lua | round trips Py multi-exec | commands Go | commands Py lua | commands Py multi-exec | median µs Go | median µs Py lua | median µs Py multi-exec |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| stat | 10 | 1 | 1 | 1 | 2 | 2 | 2 | 150 | 795 | 826 |
| stat | 1000 | 1 | 1 | 1 | 2 | 2 | 2 | 328 | 703 | 767 |
| stat | 10000 | 1 | 1 | 1 | 2 | 2 | 2 | 153 | 359 | 788 |
| readdir | 10 | 1 | 1 | 1 | 1 | 1 | 1 | 146 | 800 | 780 |
| readdir | 1000 | 1 | 1 | 1 | 1 | 1 | 1 | 2501 | 3519 | 5018 |
| readdir | 10000 | 1 | 1 | 1 | 1 | 1 | 1 | 18337 | 42986 | 55480 |
| readdirplus | 10 | 12 | 2 | 2 | 23 | 23 | 23 | 1986 | 2382 | 2295 |
| readdirplus | 1000 | 1002 | 2 | 2 | 2003.1 | 2003 | 2003 | 166619 | 53574 | 55753 |
| readdirplus | 10000 | 10002 | 2 | 2 | 20004.5 | 20003 | 20003 | 1409070 | 461517 | 502877 |
| create | 10 | 1 | 1 | 3 | 8 | 8 | 10 | 432 | 997 | 1935 |
| create | 1000 | 1 | 1 | 3 | 8 | 8 | 10 | 205 | 1000 | 2133 |
| create | 10000 | 1 | 1 | 3 | 8 | 8 | 10 | 194 | 804 | 1951 |
| mkdir | 10 | 1 | 1 | 3 | 7 | 7 | 9 | 400 | 1023 | 1771 |
| mkdir | 1000 | 1 | 1 | 3 | 7 | 7 | 9 | 197 | 1012 | 1645 |
| mkdir | 10000 | 1 | 1 | 3 | 7 | 7 | 9 | 304 | 1097 | 1987 |
| write64 | 10 | 1 | 1 | 2 | 3 | 3 | 5 | 155 | 801 | 1157 |
| write64 | 1000 | 1 | 1 | 2 | 3 | 3 | 5 | 342 | 774 | 525 |
| write64 | 10000 | 1 | 1 | 2 | 3 | 3 | 5 | 286 | 736 | 1164 |
| read64 | 10 | 1 | 1 | 1 | 1 | 1 | 1 | 122 | 753 | 717 |
| read64 | 1000 | 1 | 1 | 1 | 1 | 1 | 1 | 191 | 716 | 675 |
| read64 | 10000 | 1 | 1 | 1 | 1 | 1 | 1 | 122 | 753 | 629 |
| rename_file | 10 | 1 | 1 | 2 | 13 | 13 | 16 | 124 | 616 | 1635 |
| rename_file | 1000 | 1 | 1 | 2 | 13 | 13 | 16 | 164 | 961 | 1696 |
| rename_file | 10000 | 1 | 1 | 2 | 13 | 13 | 16 | 413 | 946 | 1379 |
| rename_dir | 10 | 1 | 1 | 2 | 13 | 13 | 16 | 366 | 415 | 1573 |
| rename_dir | 1000 | 1 | 1 | 2 | 13 | 13 | 16 | 112 | 924 | 1600 |
| rename_dir | 10000 | 1 | 1 | 2 | 13 | 13 | 16 | 398 | 934 | 1665 |
| unlink | 10 | 1 | 1 | 2 | 7 | 7 | 9 | 181 | 759 | 1426 |
| unlink | 1000 | 1 | 1 | 2 | 7 | 7 | 9 | 395 | 804 | 1313 |
| unlink | 10000 | 1 | 1 | 2 | 7 | 7 | 9 | 324 | 508 | 906 |
| rmdir_empty | 10 | 1 | 1 | 2 | 8 | 8 | 10 | 350 | 794 | 1347 |
| rmdir_empty | 1000 | 1 | 1 | 2 | 8 | 8 | 10 | 320 | 765 | 1427 |
| rmdir_empty | 10000 | 1 | 1 | 2 | 8 | 8 | 10 | 305 | 839 | 1274 |
| truncate | 10 | 3 | 3 | 2 | 10.2 | 10.05 | 7.05 | 957 | 2395 | 552 |
| truncate | 1000 | 3 | 3 | 2 | 10.05 | 10.05 | 7.05 | 306 | 2184 | 1285 |
| truncate | 10000 | 3 | 3 | 2 | 10.05 | 10.05 | 7.05 | 641 | 1896 | 1398 |

The Go run is `bench/go/harness_test.go.txt`, the Python run is `bench/bench.py`. Both write JSON
lines to `bench/results/`, and `bench/report.py` prints this table from them.

Three readings to keep in mind. With scripts on, Python runs the same commands as Go for every
mutation, at the same round trips, so what is left of the latency gap is the client stack.
`readdirplus` at N = 10 000 is 2 round trips in Python and 10 002 in Go, but about 0.46 s against
1.41 s: the Python figure is pipelined work, the Go figure is ten thousand round trips.
`truncate` is the one mutation where transactions are cheaper: one transaction against the script
plus the watched metadata update.

## Parity with the Go store

`bench/parity.py` and `bench/go/harness_test.go.txt` run the same 24-step sequence (create, write,
mkdir, symlink, rename, truncate, unlink, rmdir, replace rules, `RENAME_NOREPLACE`) against one
server, one namespace at a time. Each run ends with `bench/dump_keys.py`, which lists every key
of the namespace with timestamps normalized. Python runs once per setting
(`python3 bench/parity.py URL NS yes`, then `no`). All three runs, Go, `yes` and `no`, produced
the same outcome for every step and the same key dump, byte for byte.

## Reproducing

```bash
python3 -m pytest -q tests                       # unit and store tests, starts its own server
# Benchmarks and parity need a server and a Go checkout with the harness copied in:
redis-server --port 6399 --save "" --appendonly no &
python3 bench/bench.py redis://127.0.0.1:6399/0 20 yes > bench/results/python-lua.jsonl
redis-cli -p 6399 flushall      # the bench uses fixed namespaces; start each run on an empty server
python3 bench/bench.py redis://127.0.0.1:6399/0 20 no > bench/results/python-multi-exec.jsonl
redis-cli -p 6399 flushall
cp bench/go/harness_test.go.txt <go-repo-copy>/internal/store/harness_test.go
(cd <go-repo-copy> && KVFS_HARNESS=bench KVFS_ADDR=127.0.0.1:6399 KVFS_NS=bench KVFS_ITERS=20 \
   go test ./internal/store -run TestHarnessBench -count=1 -v | grep '^{' > bench/results/go.jsonl)
python3 bench/report.py
```

Run the Go harness in a copy of the repository, not in the repository itself: it adds a test
file. Versions used: Valkey 9.0.6, redis-py 5.2.1, pyfuse3 3.4.2, Python 3.14, Go 1.26,
rueidis v1.0.76, Linux 7.2 (Fedora).

## Tests

```bash
python3 -m pytest -q tests
```

The tests start `redis-server` (or `valkey-server`) on a free port, or use `KVFS_TEST_REDIS` when it
is set. They cover the key table (golden bytes), escaping, the metadata JSON, every replace rule,
round-trip counts, publish payloads, placement refusal, and the `dir`/`file` refusals. Every store
test runs with scripts on and off. Others check that both leave identical keys, that `auto` uses
scripts when the server allows them, that it falls back for an ACL user without `@scripting`, and
that `yes` refuses to start for that user.

## Smoke checklist (real mount)

Run against a real mount. Unit tests cannot cover these.

1. `kvfs -ns smoke /mnt/kvfs`, then `mount | grep kvfs` shows a `fuse` entry.
2. `echo hi > f && cat f` returns `hi`; `echo more >> f && cat f` returns both lines.
3. `mkdir d && echo x > d/f && ls -la d` lists `f` with size 2.
4. `mv d e && cat e/f` works; `stat e` shows the same inode number as before the rename.
5. `mv f g` over an existing `g` replaces it; `mv -T` of a file over a directory fails with `EISDIR`.
6. `rmdir e` on a non-empty directory fails with `ENOTEMPTY`.
7. `ln -s target l && readlink l` returns `target`.
8. Two mounts on two hosts (or two mountpoints): a write in one is visible in the other after the
   1 second entry timeout.
9. `kill -9` the process, `fusermount3 -u /mnt/kvfs`, remount: the tree is intact.
10. Cached bytes: `scripts/coherence.sh /mnt/a /mnt/b` in the Go `kvfs` repository (writer, reader) prints `ok` for every case, for each pair of mounts sharing one namespace.

## Layout

```
SPEC.md              the protocol, copied from the Go project, with the subset note
AGENTS.md            how the code is written (read it before editing)
kvfs/store/          keys, metadata, errors, wire, reads (store.py), and the two mutation paths:
                     lua.py (Go's scripts), multi_exec.py (WATCH/MULTI/EXEC), modes.py (--sss-mode)
kvfs/fs.py           pyfuse3 operations, inode table, errno mapping, invalidation watcher
kvfs/cli.py          flags, wiring, exit codes
tests/               pytest, against a real server
bench/               benchmark, parity, key dump, report, and the Go harness (bench/go/)
```

## Releases

The version and the release steps are in [`AGENTS.md`](https://github.com/muayyad-alsadi/pykvfs/blob/main/AGENTS.md#release). The published versions are
listed at https://pypi.org/project/pykvfs/.

## License

MIT. See `LICENSE`.
