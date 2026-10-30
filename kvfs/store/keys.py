"""The key builder. The only code that concatenates kvfs keys (AGENTS.md).

The layout is SPEC.md's, byte for byte, so a Go mount and a Python mount on the
same server read and write the same namespace.
"""

from __future__ import annotations

import re
from urllib.parse import unquote_to_bytes

MAGIC = b"kvfs"
NAMESPACE_ID = 0  # the namespace pseudo-inode, owner of the root
ROOT_PARENT_ID = 0
ROOT_ID = 1
ROOT_NAME = b"."
MAX_NAMESPACE_LEN = 64

NAMESPACE = "namespace"
DIR = "dir"
FILE = "file"
LEVELS = (NAMESPACE, DIR, FILE)

META = b"meta"
DATA = b"data"
DIRECTORY = b"dir"
COUNTER = b"counter"
PUBSUB = b"pubsub"

# Bytes that must be percent-encoded: SPEC.md, Escaping.
_ESCAPED = frozenset(b'%/"*?[]{}\\') | frozenset(range(0x20))
_NAMESPACE_RE = re.compile(rb"[A-Za-z0-9-]+")


def valid_namespace(ns: str) -> bool:
    """Reports whether ns is a legal namespace: [A-Za-z0-9-]+, at most 64 bytes."""
    raw = ns.encode()
    return 0 < len(raw) <= MAX_NAMESPACE_LEN and _NAMESPACE_RE.fullmatch(raw) is not None


def escape_name(name: bytes) -> bytes:
    """Percent-encodes the bytes SPEC.md lists, with uppercase hex; all else verbatim."""
    out = bytearray()
    for byte in name:
        if byte in _ESCAPED:
            out += b"%%%02X" % byte
        else:
            out.append(byte)
    return bytes(out)


def unescape_name(text: bytes) -> bytes:
    """Decodes escape_name. Only %XX is an escape; '+' is left alone."""
    return unquote_to_bytes(text)


def key(ns: bytes, level: str, owner: int, name: bytes, suffix: bytes) -> bytes:
    """Builds the key of one record of the entry (owner, name). Every key is built here."""
    base = escape_name(name)
    if level == FILE:
        return b"kvfs/{%s/%d/%s}/%s" % (ns, owner, base, suffix)
    if level == DIR:
        return b"kvfs/{%s/%d}/%s/%s" % (ns, owner, base, suffix)
    if level == NAMESPACE:
        return b"kvfs/{%s}/%d/%s/%s" % (ns, owner, base, suffix)
    raise ValueError(f"unknown sharding level {level!r}: want namespace, dir or file")


def invalidate_payload(owner: int, name: bytes) -> bytes:
    """The pub/sub payload that addresses one entry: '<owner>/<escaped name>'."""
    return b"%d/%s" % (owner, escape_name(name))


def parse_invalidate(payload: bytes) -> tuple[int, bytes]:
    """Decodes invalidate_payload into (owner, name)."""
    owner, _, text = payload.partition(b"/")
    return int(owner), unescape_name(text)
