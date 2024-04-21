from dataclasses import dataclass
from typing import Any


@dataclass
class Message:
    parsed: Any
    size: int
