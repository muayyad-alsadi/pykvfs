"""Dumps every key of one namespace, sorted, with timestamps normalized, for a byte diff.

Usage: python3 bench/dump_keys.py REDIS_URL NAMESPACE [--delete]
Timestamps (atime, mtime, ctime) are replaced by T: the two implementations run at different times.
"""

import re
import sys

import redis

TIMESTAMP = re.compile(rb'"(atime|mtime|ctime)":\d+')


def normalize(raw: bytes) -> bytes:
    return TIMESTAMP.sub(rb'"\1":T', raw)


def dump(client: redis.Redis, ns: str) -> list[str]:
    lines = []
    for key in sorted(client.scan_iter(match=f"kvfs/{{{ns}*", count=1000)):
        kind = client.type(key).decode()
        if kind == "string":
            value = normalize(client.get(key))
        elif kind == "hash":
            fields = client.hgetall(key)
            value = b";".join(k + b"=" + v for k, v in sorted(fields.items()))
        else:
            value = b""
        lines.append(f"{key.decode()} {kind} {value.decode(errors='replace')}")
    return sorted(lines)


def main(argv: list[str]) -> None:
    url, ns = argv[1], argv[2]
    client = redis.Redis.from_url(url)
    for line in dump(client, ns):
        print(line)
    if "--delete" in argv:
        keys = list(client.scan_iter(match=f"kvfs/{{{ns}*", count=1000))
        if keys:
            client.delete(*keys)


if __name__ == "__main__":
    main(sys.argv)
