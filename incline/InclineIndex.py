from dataclasses import dataclass
from typing import Any


@dataclass
class InclineIndex:
    name: str
    path: str = ''
    value: Any = None
