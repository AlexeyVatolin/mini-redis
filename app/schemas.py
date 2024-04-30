import asyncio
import datetime
from dataclasses import dataclass
from typing import Any

PEERNAME = tuple[str, int, int, int]


@dataclass
class Connection:
    peername: PEERNAME
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    offset: int = 0

    @staticmethod
    def create(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> "Connection":
        return Connection(writer.get_extra_info("peername"), reader, writer)


@dataclass
class WaitTrigger:
    event: asyncio.Event
    num_replicas: int
    master_offset: int

    @staticmethod
    def create(num_replicas: int, master_offset: int) -> "WaitTrigger":
        return WaitTrigger(asyncio.Event(), num_replicas, master_offset)


@dataclass(order=True, frozen=True)
class EntryId:
    timestamp: int | float
    sequence_number: int | float

    @staticmethod
    def from_string(s: str) -> "EntryId":
        timestamp, sequence_number = s.split("-")
        return EntryId(int(timestamp), int(sequence_number))

    def __str__(self) -> str:
        return f"{self.timestamp}-{self.sequence_number}"


@dataclass
class StreamTrigger:
    event: asyncio.Event
    key: str
    entry_id: EntryId


@dataclass
class StorageValue:
    value: Any
    expired_time: datetime.datetime | None = None
