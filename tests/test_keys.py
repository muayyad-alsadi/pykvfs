"""Golden key strings: the schema contract. Each row is the Go store's output (internal/store/keys.go)."""

import pytest

from kvfs.store import keys
from kvfs.store.meta import Child, Meta, encode_child, parse_child

KEY_TABLE = [
    # (namespace, level, parent, name, suffix, want)
    ("default", "namespace", 0, b".", "counter", "kvfs/{default}/0/./counter"),
    ("default", "namespace", 0, b".", "pubsub", "kvfs/{default}/0/./pubsub"),
    ("default", "namespace", 0, b".", "meta", "kvfs/{default}/0/./meta"),
    ("default", "namespace", 0, b".", "dir", "kvfs/{default}/0/./dir"),
    ("default", "namespace", 1, b"README.md", "data", "kvfs/{default}/1/README.md/data"),
    ("default", "dir", 1, b"README.md", "data", "kvfs/{default/1}/README.md/data"),
    ("default", "file", 1, b"README.md", "data", "kvfs/{default/1/README.md}/data"),
    ("default", "dir", 0, b".", "dir", "kvfs/{default/0}/./dir"),
    ("default", "file", 0, b".", "dir", "kvfs/{default/0/.}/dir"),
    ("default", "namespace", 3, b"c%2Fd", "meta", "kvfs/{default}/3/c%252Fd/meta"),
    ("default", "namespace", 3, b"a/b", "meta", "kvfs/{default}/3/a%2Fb/meta"),
    ("default", "namespace", 3, b"x y:z", "meta", "kvfs/{default}/3/x y:z/meta"),
    ("default", "namespace", 3, "café".encode(), "meta", "kvfs/{default}/3/café/meta"),
]


@pytest.mark.parametrize("ns, level, parent, name, suffix, want", KEY_TABLE)
def test_key_matches_go_store(ns, level, parent, name, suffix, want):
    assert keys.key(ns.encode(), level, parent, name, suffix.encode()).decode() == want


ESCAPE_TABLE = [
    (b"plain", b"plain"),
    (b"a/b", b"a%2Fb"),
    (b'q"uote', b"q%22uote"),
    (b"star*", b"star%2A"),
    (b"what?", b"what%3F"),
    (b"[x]", b"%5Bx%5D"),
    (b"{tag}", b"%7Btag%7D"),
    (b"back\\slash", b"back%5Cslash"),
    (b"pct%", b"pct%25"),
    (b"tab\there", b"tab%09here"),
    (b"colon:space ", b"colon:space "),
    ("é".encode(), "é".encode()),
    (b"..", b".."),
]


@pytest.mark.parametrize("name, want", ESCAPE_TABLE)
def test_escape_name(name, want):
    assert keys.escape_name(name) == want


@pytest.mark.parametrize("name", [b"a/b", b"%", b"x*?[]{}\"\\", b"\x00\x1f", "é :".encode(), b"+ plus"])
def test_escape_round_trips(name):
    assert keys.unescape_name(keys.escape_name(name)) == name


def test_unescape_keeps_plus_as_plus():
    assert keys.unescape_name(b"a+b") == b"a+b"


def test_invalidate_payload_matches_go():
    assert keys.invalidate_payload(1, b"c/d") == b"1/c%2Fd"
    assert keys.parse_invalidate(b"1/c%2Fd") == (1, b"c/d")


@pytest.mark.parametrize("ns, ok", [
    ("default", True),
    ("A-z-0", True),
    ("", False),
    ("a_b", False),
    ("a/b", False),
    ("x" * 64, True),
    ("x" * 65, False),
])
def test_valid_namespace(ns, ok):
    assert keys.valid_namespace(ns) is ok


def test_unknown_level_is_refused():
    with pytest.raises(ValueError):
        keys.key(b"default", "directory", 0, b".", b"meta")


def test_meta_json_is_go_compatible():
    meta = Meta(id=1, type="dir", mode=0o755, uid=1000, gid=1000, nlink=1, mtime=5, ctime=6, atime=7)
    assert meta.to_json() == (
        b'{"id":1,"type":"dir","mode":493,"uid":1000,"gid":1000,"nlink":1,"mtime":5,"ctime":6,"atime":7}'
    )


def test_meta_round_trip_ignores_size():
    meta = Meta(id=2, type="file", mode=0o644, uid=1, gid=2, nlink=1, mtime=3, ctime=4, atime=5, size=99)
    back = Meta.from_json(meta.to_json())
    assert back.size == 0
    assert back.id == 2 and back.mode == 0o644


@pytest.mark.parametrize("typ, mode, want", [
    ("file", 0o644, 0o100644),
    ("dir", 0o755, 0o040755),
    ("link", 0o777, 0o120777),
    ("file", 0o4755, 0o100755),
])
def test_fuse_mode(typ, mode, want):
    assert Meta(id=1, type=typ, mode=mode, uid=0, gid=0, nlink=1, mtime=0, ctime=0, atime=0).fuse_mode() == want


@pytest.mark.parametrize("value, want", [
    (b"f:2", Child(b"a", "file", 2)),
    (b"d:42", Child(b"a", "dir", 42)),
    (b"l:9", Child(b"a", "link", 9)),
    (b"x:9", Child(b"a", "", 9)),
    (b"f:2:p", Child(b"a", "", 2)),
    (b"garbage", Child(b"a", "", 0)),
])
def test_parse_child(value, want):
    assert parse_child(b"a", value) == want


def test_encode_child_matches_listing_value():
    assert encode_child("dir", 42) == b"d:42"
