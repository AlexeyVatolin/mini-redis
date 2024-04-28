import dataclasses
import datetime
import time
from typing import Any

from app.exception import StreamIdOrderError, StreamIDTooLowError


@dataclasses.dataclass
class StorageValue:
    value: Any
    expired_time: datetime.datetime | None = None


class Stream:
    def __init__(self) -> None:
        self._entries: dict = {}
        self._last_entry = (0, 0)

    def xadd(self, id_: str) -> str:
        if id_ == "*-*":
            timestamp, sequence_number = (time.time_ns() // 1_000_000, 0)
        elif id_.endswith("*"):
            timestamp = int(id_.split("-")[0])
            if self._last_entry[0] == timestamp:
                sequence_number = self._last_entry[1] + 1
            else:
                sequence_number = 0
        else:
            timestamp, sequence_number = map(int, id_.split("-"))

            if timestamp <= 0 and sequence_number <= 0:
                raise StreamIDTooLowError

            if (
                timestamp < self._last_entry[0]
                or timestamp == self._last_entry[0]
                and sequence_number <= self._last_entry[1]
            ):
                raise StreamIdOrderError()
        self._last_entry = (timestamp, sequence_number)
        return f"{timestamp}-{sequence_number}"


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
