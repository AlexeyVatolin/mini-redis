import asyncio
from dataclasses import dataclass

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
