"""The store's errors. fs.py maps them to errno in one place."""


class KvfsError(Exception):
    """Base class of every error the store raises on purpose."""


class NotFound(KvfsError):
    """The entry, or its parent, does not exist."""


class Exists(KvfsError):
    """The name is already taken."""


class NotEmpty(KvfsError):
    """The directory has children."""


class NotDir(KvfsError):
    """The entry is not a directory."""


class IsDir(KvfsError):
    """The entry is a directory."""


class Busy(KvfsError):
    """Other writers kept changing the same keys; the operation gave up."""


class Placement(KvfsError):
    """The namespace was written at another sharding level, or at more than one."""


class ScriptsRefused(KvfsError):
    """The server does not allow server-side scripts, and the mount required them."""
