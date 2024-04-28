from __future__ import annotations

import asyncio
import base64
import contextlib
import datetime
from typing import TYPE_CHECKING, Any

from app.exception import RedisError
from app.redis_serde import BulkString, ErrorString, Message, RDBString, SimpleString
from app.schemas import PEERNAME, WaitTrigger
from app.storage import Storage, StorageValue, Stream
from app.utils import random_id

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
        self.master_id = random_id(40) if self._server.is_master else None

    async def handle(self, message: Message, peername: PEERNAME) -> list[Any]:
        if not isinstance(message.parsed, list):
            return []

        response = await self._handle_impl(message, peername)
        if not self._server.is_master:
            if message.parsed[0].lower() in {"replconf", "info", "get"}:
                return response
            return []

        return response

    async def _handle_impl(self, message: Message, peername: PEERNAME) -> list[Any]:
        from app.server import MasterServer

        command = message.parsed[0].lower()
        match command:
            case "ping":
                return [SimpleString("PONG")]
            case "echo":
                return [BulkString(message.parsed[1])]
            case "set":
                if len(message.parsed) < 3:
                    return [ErrorString("Wrong number of arguments for 'set' command")]
                if isinstance(self._server, MasterServer):
                    asyncio.create_task(self._server.propagate(message))

                key, expired_time = message.parsed[1], None
                if len(message.parsed) == 5 and message.parsed[3].lower() == "px":
                    expired_time = datetime.datetime.now() + datetime.timedelta(
                        milliseconds=int(message.parsed[4])
                    )
                self._storage[key] = StorageValue(message.parsed[2], expired_time)
                return [SimpleString("OK")]
            case "get":
                value = self._storage[message.parsed[1]]
                return [BulkString(value) if value else None]
            case "xadd":
                if len(message.parsed) < 5:
                    return [ErrorString("Wrong number of arguments for 'xadd' command")]
                stream_key, stream_id = message.parsed[1], message.parsed[2]
                if self._storage[stream_key] is None:
                    self._storage[stream_key] = StorageValue(Stream())
                stream: Stream = self._storage[stream_key]
                try:
                    return [BulkString(stream.xadd(stream_id, message.parsed[3:]))]
                except RedisError as e:
                    return [ErrorString(e.message)]
            case "xrange":
                if len(message.parsed) != 4:
                    return [ErrorString("Wrong number of arguments for 'xrange' command")]
                stream_key = message.parsed[1]
                stream: Stream = self._storage[stream_key]
                return [stream.xrange(message.parsed[2], message.parsed[3])]
            case "xread":
                if len(message.parsed) < 4 or len(message.parsed) % 2 != 0:
                    return [ErrorString("Wrong number of arguments for 'xread' command")]
                args = message.parsed[2:]

                return [
                    [
                        [BulkString(stream_key), self._storage[stream_key].xread(start)]
                        for stream_key, start in zip(
                            args[: len(args) // 2], args[len(args) // 2 :]
                        )
                    ]
                ]
            case "type":
                value = self._storage[message.parsed[1]]
                if isinstance(value, str):
                    return [SimpleString("string")]
                elif isinstance(value, Stream):
                    return [SimpleString("stream")]
                return [SimpleString("none")]
            case "info":
                if len(message.parsed) != 2 or message.parsed[1].lower() != "replication":
                    return [ErrorString("Wrong arguments for 'info' command")]
                role = "master" if self._server.is_master else "slave"
                return [
                    BulkString(
                        f"# Replication\nrole:{role}\nmaster_replid:{self.master_id}\nmaster_repl_offset:{self._server.offset}"
                    )
                ]
            case "replconf":
                if len(message.parsed) == 3 and message.parsed[1].lower() == "getack":
                    return [
                        [
                            BulkString("REPLCONF"),
                            BulkString("ACK"),
                            BulkString(str(self._server.offset)),
                        ]
                    ]
                if len(message.parsed) == 3 and message.parsed[1].lower() == "ack":
                    if isinstance(self._server, MasterServer):
                        self._server.store_offset(peername, int(message.parsed[2]))
                    return []

                return [SimpleString("OK")]
            case "psync":
                return [
                    SimpleString(f"FULLRESYNC {self.master_id} {self._server.offset}"),
                    RDBString(default_rdb),
                ]
            case "wait":
                if len(message.parsed) != 3:
                    return [ErrorString("Wrong arguments for 'wait' command")]
                if not isinstance(self._server, MasterServer):
                    return [ErrorString("Only available for master")]

                num_replicas, timeout = map(int, message.parsed[1:])
                num_replicas = min(num_replicas, self._server.num_replicas)
                master_offset = self._server.offset
                synced_replicas = self._server.count_synced_replicas(master_offset)
                if num_replicas <= synced_replicas:
                    return [self._server.num_replicas]

                trigger = WaitTrigger.create(num_replicas, master_offset)
                self._server.register_trigger(trigger)

                await self._server.propagate(
                    Message.from_parsed(
                        [BulkString("REPLCONF"), BulkString("GETACK"), BulkString("*")]
                    )
                )

                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(trigger.event.wait(), timeout / 1000)

                return [self._server.count_synced_replicas(master_offset)]
            case "config":
                subcommand = message.parsed[1].lower()
                if subcommand == "get":
                    key = message.parsed[2].lower()
                    if key not in {"dir", "dbfilename"}:
                        return [ErrorString(f"Unknown config key {key}")]
                    return [[BulkString(key), BulkString(self._server.config[key])]]
                return [ErrorString("Unknown config subcommand {subcommand}")]
            case "keys":
                subcommand = message.parsed[1].lower()
                if subcommand == "*":
                    return [[BulkString(key) for key in self._storage]]
                return [ErrorString("Unknown keys subcommand {subcommand}")]
            case _:
                return [ErrorString("Unknown command")]

    def need_store_connection(self, message: Message) -> bool:
        if not isinstance(message.parsed, list) or len(message.parsed) == 0:
            return False
        command = message.parsed[0].lower()
        return command == "psync"
