"""How mutations reach the server: the `--sss-mode` flag.

"sss" is server-side scripts, the Lua scripts the Go store runs. The flag answers
one question, "does this server support them?":

- `yes`: use them. Refuse to start if the server does not allow them.
- `no`: never use them, even when the server allows them.
- `auto` (the default): use them if the server allows them, otherwise use
  WATCH/MULTI/EXEC transactions.

Both kinds of mutation write the same keys, so the choice does not change what is stored.
"""

from __future__ import annotations

from redis.asyncio import ConnectionPool, Redis
from redis.exceptions import ResponseError

from . import keys
from .errors import ScriptsRefused
from .lua import LuaStore
from .multi_exec import MultiExecStore
from .store import Store, check_level

YES = "yes"
NO = "no"
AUTO = "auto"
SSS_MODES = (YES, NO, AUTO)


async def open_store(pool: ConnectionPool, ns: str, level: str = keys.NAMESPACE, sss_mode: str = AUTO) -> Store:
    """The store for an --sss-mode value. A bad value, level or namespace is refused before any server call."""
    if sss_mode not in SSS_MODES:
        raise ValueError(f"unknown --sss-mode {sss_mode!r}: want yes, no or auto")
    check_level(ns, level)
    if sss_mode == NO:
        return MultiExecStore(pool, ns, level)
    if await server_allows_scripts(pool):
        return LuaStore(pool, ns, level)
    if sss_mode == YES:
        raise ScriptsRefused("this server does not allow server-side scripts; start with --sss-mode=auto to run without them")
    return MultiExecStore(pool, ns, level)


async def server_allows_scripts(pool: ConnectionPool) -> bool:
    """One EVAL of a script that names no key. A refusal (scripting disabled, an ACL without it) means no."""
    client = Redis(connection_pool=pool)
    try:
        return await client.execute_command("EVAL", "return 1", 0) == 1
    except ResponseError:
        return False
    finally:
        await client.aclose()
