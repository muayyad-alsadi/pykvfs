"""Metadata documents and listing values, byte-compatible with the Go store.

Metadata is a JSON document at the entry's `meta` key. A directory's listing is a
hash: field = child basename, value = '<type code>:<inode>', e.g. 'd:42'.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

TYPE_FILE = "file"
TYPE_DIR = "dir"
TYPE_LINK = "link"

_CODE = {TYPE_FILE: b"f", TYPE_DIR: b"d", TYPE_LINK: b"l"}
_TYPE = {code: typ for typ, code in _CODE.items()}
_TYPE_BITS = {TYPE_DIR: 0o040000, TYPE_LINK: 0o120000, TYPE_FILE: 0o100000}


@dataclass
class Meta:
    """The metadata document. Size is not stored: it is the length of the data key."""

    id: int
    type: str
    mode: int  # permission bits only
    uid: int
    gid: int
    nlink: int
    mtime: int
    ctime: int
    atime: int
    size: int = 0  # filled from STRLEN by the store, never serialized

    def to_json(self) -> bytes:
        """Serializes with the Go field order and no spaces, so both stores write the same bytes."""
        doc = {
            "id": self.id,
            "type": self.type,
            "mode": self.mode,
            "uid": self.uid,
            "gid": self.gid,
            "nlink": self.nlink,
            "mtime": self.mtime,
            "ctime": self.ctime,
            "atime": self.atime,
        }
        return json.dumps(doc, separators=(",", ":")).encode()

    @classmethod
    def from_json(cls, raw: bytes) -> Meta:
        doc = json.loads(raw)
        return cls(
            id=doc["id"],
            type=doc["type"],
            mode=doc["mode"],
            uid=doc["uid"],
            gid=doc["gid"],
            nlink=doc["nlink"],
            mtime=doc["mtime"],
            ctime=doc["ctime"],
            atime=doc["atime"],
        )

    def fuse_mode(self) -> int:
        """The st_mode the kernel sees: type bits from the type, permission bits from mode."""
        return (self.mode & 0o777) | _TYPE_BITS.get(self.type, 0)


def new_meta(inode: int, typ: str, mode: int, uid: int, gid: int, now: int) -> Meta:
    """A fresh entry: one link, all three timestamps set to now."""
    return Meta(id=inode, type=typ, mode=mode, uid=uid, gid=gid, nlink=1, mtime=now, ctime=now, atime=now)


@dataclass(frozen=True)
class Child:
    """One listing field. type is '' when the code is unreadable or the field is pending."""

    name: bytes
    type: str
    id: int


def type_code(typ: str) -> bytes:
    """The one-byte listing code of a metadata type: b'f', b'd' or b'l'."""
    return _CODE[typ]


def encode_child(typ: str, inode: int) -> bytes:
    return type_code(typ) + b":" + str(inode).encode()


def parse_child(name: bytes, value: bytes) -> Child:
    """Decodes one listing field. A malformed or marked field keeps its name, with unknown type."""
    code, sep, rest = value.partition(b":")
    ident, colon, _marker = rest.partition(b":")
    known = _TYPE.get(code, "") if sep and not colon else ""
    return Child(name, known, int(ident) if ident.isdigit() else 0)
