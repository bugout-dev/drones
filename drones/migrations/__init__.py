from typing import Callable, Dict, Optional
from uuid import UUID

from . import add_error_tag

ACTIONS = ["up", "down"]

MIGRATIONS: Dict[str, Dict[str, Callable[[Optional[UUID], bool], None]]] = {
    "error_tags": {"up": add_error_tag.upgrade, "down": add_error_tag.downgrade}
}
