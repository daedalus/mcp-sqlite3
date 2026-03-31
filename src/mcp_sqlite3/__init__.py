__version__ = "0.1.2"
__all__ = ["mcp_server"]

from typing import TYPE_CHECKING

from .mcp import mcp_server

if TYPE_CHECKING:
    from .mcp import *  # noqa: F403
