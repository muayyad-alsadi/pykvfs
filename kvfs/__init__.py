"""pykvfs: a Redis/Valkey-backed FUSE filesystem, wire-compatible with the Go kvfs at namespace sharding.

The package has three layers (AGENTS.md): kvfs.store owns every Redis call and
the key builder, kvfs.fs owns the FUSE nodes and errno mapping, kvfs.cli owns
flags and wiring.
"""

__version__ = "0.2.1"
