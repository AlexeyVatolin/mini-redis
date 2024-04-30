from __future__ import annotations

import asyncio
import base64
import contextlib
import datetime
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, AsyncGenerator

from app.exception import RedisError
from app.redis_serde import BulkString, ErrorString, Message, RDBString, SimpleString
from app.schemas import PEERNAME, EntryId, StorageValue, StreamTrigger, WaitTrigger
from app.storage import Storage, Stream
from app.utils import to_pairs

if TYPE_CHECKING:
    from app.server import RedisServer

default_rdb = base64.b64decode(
    "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="
)


class RedisCommandHandler:
    def __init__(self, server: RedisServer, storage: Storage | None) -> None:
        self._server = server
        if storage is None:
            self._storage = Storage()
        else:
            self._storage = storage

    async def handle(self, message: Message, peername: PEERNAME) -> list[Any]:
        if not isinstance(message.parsed, list):
            return []

        command_class: ICommand | None = {
            "ping": PingCommand,
            "echo": EchoCommand,
            "set": SetCommand,
            "get": GetCommand,
            "xadd": XAddCommand,
            "xrange": XRangeCommand,
            "xread": XReadCommand,
            "type": TypeCommand,
            "info": InfoCommand,
            "replconf": ReplconfCommand,
            "psync": PsyncCommand,
            "wait": WaitCommand,
            "config": ConfigCommand,
            "keys": KeysCommand,
        }.get(message.parsed[0].lower())

        if command_class is None:
            return [ErrorString("Unknown command")]

        response = [
            item
            async for item in command_class(self._server, self._storage, peername).execute(message)
        ]

        if not self._server.is_master:
            if (
                issubclass(command_class, ReplconfCommand)
                or issubclass(command_class, InfoCommand)
                or issubclass(command_class, GetCommand)
            ):
                return response
            return []

        return response

    def need_store_connection(self, message: Message) -> bool:
        if not isinstance(message.parsed, list) or len(message.parsed) == 0:
            return False
        command = message.parsed[0].lower()
        return command == "psync"


class ICommand(ABC):
    def __init__(self, server: RedisServer, storage: Storage, peername: PEERNAME) -> None:
        self._server = server
        self._storage = storage
        self._peername = peername

    @abstractmethod
    async def execute(self, message: Message) -> AsyncGenerator[list[str] | str, None]:
        raise NotImplementedError

    def _propagate(self, message: Message) -> None:
        from app.server import MasterServer

        if isinstance(self._server, MasterServer):
            asyncio.create_task(self._server.propagate(message))


def bulk_string_wrap(xrange: list | None) -> list | None:
    if not xrange:
        return None
    return [
        BulkString(v) if isinstance(v, str) or isinstance(v, EntryId) else bulk_string_wrap(v)
        for v in xrange
    ]


class PingCommand(ICommand):
    async def execute(self, message: Message) -> AsyncGenerator[list[str] | str, None]:
        yield SimpleString("PONG")


class EchoCommand(ICommand):
    async def execute(self, message: Message) -> AsyncGenerator[list[str] | str, None]:
        yield BulkString(message.parsed[1])


class SetCommand(ICommand):
    async def execute(self, message: Message) -> AsyncGenerator[list[str] | str, None]:
        match message.parsed[1:]:
            case [key, value]:
                await self._set_value(message, key, value)
                yield SimpleString("OK")
            case [key, value, "px", expired_time]:
                await self._set_value(
                    message,
                    key,
                    value,
                    expired_time=datetime.datetime.now()
                    + datetime.timedelta(milliseconds=int(expired_time)),
                )
                yield SimpleString("OK")
            case _:
                yield ErrorString("Wrong number of arguments for'set' command")

    async def _set_value(
        self, message: Message, key: str, value: str, expired_time: datetime.datetime | None = None
    ) -> None:
        self._propagate(message)
        self._storage[key] = StorageValue(value, expired_time)


class GetCommand(ICommand):
    async def execute(self, message: Message) -> AsyncGenerator[list[str] | str | None, None]:
        match message.parsed[1:]:
            case [key]:
                value = self._storage[key]
                yield BulkString(value) if value else None
            case _:
                yield ErrorString("Wrong number of arguments for 'get' command")


class XAddCommand(ICommand):
    async def execute(self, message: Message) -> AsyncGenerator[list[str] | str, None]:
        match message.parsed[1:]:
            case [stream_key, entry_id, *entries]:
                self._propagate(message)

                try:
                    yield BulkString(self._xadd(stream_key, entry_id, entries))
                except RedisError as e:
                    yield ErrorString(e.message)
            case _:
                yield ErrorString("Wrong number of arguments for 'xadd' command")

    def _xadd(self, stream_key: str, entry_id: str, entries: list[str]) -> EntryId:
        if self._storage[stream_key] is None:
            self._storage[stream_key] = StorageValue(Stream())
        stream: Stream = self._storage[stream_key]

        result = stream.xadd(entry_id, entries)
        self._server.check_stream_triggers(stream_key, result)
        return result


class XRangeCommand(ICommand):
    async def execute(self, message: Message) -> AsyncGenerator[list[str] | str, None]:
        match message.parsed[1:]:
            case [stream_key, start, end]:
                stream: Stream = self._storage[stream_key]
                yield bulk_string_wrap(stream.xrange(start, end))
            case _:
                yield ErrorString("Wrong number of arguments for 'xrange' command")


class XReadCommand(ICommand):
    async def execute(self, message: Message) -> AsyncGenerator[list[str] | str, None]:
        match message.parsed[1:]:
            case ["streams", *streams]:
                yield bulk_string_wrap(await self._xread(streams))
            case ["block", block_time, "streams", *streams]:
                yield bulk_string_wrap(await self._xread(streams, float(block_time) / 1000.0))
            case _:
                yield ErrorString("Wrong number of arguments for 'xread' command")

    async def _xread(self, streams: list[str], block_time: float | None = None) -> list:
        stream_entry_id = [
            (stream_key, EntryId.from_string(start, self._storage[stream_key]))
            for stream_key, start in to_pairs(streams)
        ]
        if block_time is not None:
            trigger = StreamTrigger(
                asyncio.Event(),
                stream_entry_id[0][0],
                stream_entry_id[0][1],
            )
            self._server.register_stream_trigger(trigger)
            if block_time > 0:
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(trigger.event.wait(), timeout=block_time)
            else:
                await trigger.event.wait()

        result = []
        for stream_key, entry_id in stream_entry_id:
            stream: Stream | None = self._storage[stream_key]
            if stream is None:
                continue
            stream_items = stream.xread(entry_id)
            if stream_items:
                result.append([stream_key, stream_items])
        if not result:
            return [None]
        return result


class TypeCommand(ICommand):
    async def execute(self, message: Message) -> AsyncGenerator[list[str] | str, None]:
        value = self._storage[message.parsed[1]]
        if isinstance(value, str):
            yield SimpleString("string")
        elif isinstance(value, Stream):
            yield SimpleString("stream")
        yield SimpleString("none")


class InfoCommand(ICommand):
    async def execute(self, message: Message) -> AsyncGenerator[list[str] | str, None]:
        match [m.lower() for m in message.parsed[1:]]:
            case ["replication"]:
                role = "master" if self._server.is_master else "slave"
                yield BulkString(
                    f"# Replication\nrole:{role}\nmaster_replid:{self._server.master_id}\nmaster_repl_offset:{self._server.offset}"
                )
            case _:
                yield ErrorString("Wrong arguments for 'info' command")


class ReplconfCommand(ICommand):
    async def execute(self, message: Message) -> AsyncGenerator[list[str] | str, None]:
        match [m.lower() for m in message.parsed[1:]]:
            case ["getack", "*"]:
                yield [
                    BulkString("REPLCONF"),
                    BulkString("ACK"),
                    BulkString(str(self._server.offset)),
                ]
            case ["ack", offset]:
                from app.server import MasterServer

                if isinstance(self._server, MasterServer):
                    self._server.store_offset(self._peername, int(offset))
                    # yield SimpleString("OK")
            case ["listening-port", _]:
                yield SimpleString("OK")
            case ["capa", "psync2"]:
                yield SimpleString("OK")
            case _:
                yield ErrorString("Wrong arguments for'replconf' command")


class PsyncCommand(ICommand):
    async def execute(self, message: Message) -> AsyncGenerator[list[str] | str | bytes, None]:
        match message.parsed[1:]:
            case ["?", "-1"]:
                yield SimpleString(f"FULLRESYNC {self._server.master_id} {self._server.offset}")
                yield RDBString(default_rdb)
            case _:
                yield ErrorString("Wrong arguments for 'psync' command")


class WaitCommand(ICommand):
    async def execute(self, message: Message) -> AsyncGenerator[list[str] | str, None]:
        from app.server import MasterServer

        if not isinstance(self._server, MasterServer):
            yield ErrorString("Only available for master")

        match message.parsed[1:]:
            case [num_replicas, timeout]:
                num_replicas, timeout = map(int, (num_replicas, timeout))
                num_replicas = min(num_replicas, self._server.num_replicas)
                master_offset = self._server.offset
                synced_replicas = self._server.count_synced_replicas(master_offset)
                if num_replicas <= synced_replicas:
                    yield self._server.num_replicas
                    return

                trigger = WaitTrigger.create(num_replicas, master_offset)
                self._server.register_stream_trigger(trigger)

                await self._server.propagate(
                    Message.from_parsed(
                        [BulkString("REPLCONF"), BulkString("GETACK"), BulkString("*")]
                    )
                )

                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(trigger.event.wait(), timeout / 1000)

                yield self._server.count_synced_replicas(master_offset)

            case _:
                yield ErrorString("Wrong arguments for 'wait' command")


class ConfigCommand(ICommand):
    async def execute(self, message: Message) -> AsyncGenerator[list[str] | str, None]:
        match [m.lower() for m in message.parsed[1:]]:
            case ["get", key]:
                if key not in {"dir", "dbfilename"}:
                    yield ErrorString(f"Unknown config key {key}")
                yield [BulkString(key), BulkString(self._server.config[key])]
            case _:
                yield ErrorString("Wrong arguments for 'config' command")


class KeysCommand(ICommand):
    async def execute(self, message: Message) -> AsyncGenerator[list[str] | str, None]:
        subcommand = message.parsed[1].lower()
        if subcommand == "*":
            yield [BulkString(key) for key in self._storage]
        yield ErrorString("Unknown keys subcommand {subcommand}")
