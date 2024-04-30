import datetime
import math
import time
from typing import Any, Literal

from app.exception import StreamIdOrderError, StreamIDTooLowError
from app.schemas import EntryId, StorageValue


class Stream:
    def __init__(self) -> None:
        self._entries: dict = {}
        self._last_entry = EntryId(0, 0)

    def _vaidate_entry_id(self, id_: str) -> EntryId:
        if id_ == "*":
            timestamp, sequence_number = (time.time_ns() // 1_000_000, 0)
        elif id_.endswith("*"):
            timestamp = int(id_.split("-")[0])
            if self._last_entry.timestamp == timestamp:
                sequence_number = self._last_entry.sequence_number + 1
            else:
                sequence_number = 0
        else:
            timestamp, sequence_number = map(int, id_.split("-"))

            if timestamp <= 0 and sequence_number <= 0:
                raise StreamIDTooLowError

            if (
                timestamp < self._last_entry.timestamp
                or timestamp == self._last_entry.timestamp
                and sequence_number <= self._last_entry.sequence_number
            ):
                raise StreamIdOrderError
        return EntryId(timestamp, sequence_number)

    def xadd(self, entry_id: str, value: list[str]) -> EntryId:
        self._last_entry = self._vaidate_entry_id(entry_id)
        self._entries[self._last_entry] = [v for v in value]
        return self._last_entry

    def xrange(self, start: str, end: str) -> list[list[str]]:
        start_key, end_key = self._make_key(start, "start"), self._make_key(end, "end")
        return [[key, self._entries[key]] for key in self._entries if start_key <= key <= end_key]

    def xread(self, start: str) -> list[list[str]]:
        start_key = self._make_key(start, "start")
        return [[key, self._entries[key]] for key in self._entries if key > start_key]

    @staticmethod
    def _make_key(key: str, position: Literal["start", "end"]) -> EntryId:
        if key == "-":
            return EntryId(0, 0)
        if key == "+":
            return EntryId(math.inf, math.inf)
        return (
            EntryId(int(key), math.inf if position == "end" else 0)
            if "-" not in key
            else EntryId(*map(int, key.split("-")))
        )


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
