import dataclasses
import datetime
from typing import Any


@dataclasses.dataclass
class StorageValue:
    value: Any
    expired_time: datetime.datetime | None = None


class Storage:
    def __init__(self) -> None:
        self._storage: dict[str, StorageValue] = {}

    def __setitem__(self, key: str, value: StorageValue) -> None:
        self._storage[key] = value

    def __getitem__(self, key: str) -> Any:
        if key in self._storage and self._storage[key]:
            if (
                self._storage[key].expired_time is None
                or self._storage[key].expired_time > datetime.datetime.now()
            ):
                return self._storage[key].value
            else:
                del self._storage[key]
        return None

    def __iter__(self) -> Any:
        return iter(self._storage)

    def __len__(self) -> int:
        return len(self._storage)
