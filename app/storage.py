import dataclasses
import datetime
from typing import Any


@dataclasses.dataclass
class StorageValue:
    expired_time: datetime.datetime | None
    value: Any


class Storage:
    def __init__(self) -> None:
        self._storage: dict[str, StorageValue] = {}

    def __setitem__(self, key: str, value: StorageValue) -> None:
        self._storage[key] = value

    def __getitem__(self, key: str) -> Any:
        if self._storage[key]:
            if (
                self._storage[key].expired_time is None
                or self._storage[key].expired_time > datetime.datetime.now()
            ):
                return self._storage[key].value
            else:
                del self._storage[key]
        return None
