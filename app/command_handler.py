from __future__ import annotations

import asyncio
import base64
import contextlib
import dataclasses
import datetime
from typing import TYPE_CHECKING, Any

from app.redis_serde import BulkString, ErrorString, Message, RDBString, SimpleString
from app.schemas import PEERNAME, WaitTrigger
from app.utils import random_id

if TYPE_CHECKING:
    from app.server import RedisServer

default_rdb = base64.b64decode(
    "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="
)


@dataclasses.dataclass
class StorageValue:
    expired_time: datetime.datetime | None
    value: Any


STORAGE: dict[str, StorageValue] = {}


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


class RedisCommandHandler:
    def __init__(self, server: RedisServer) -> None:
        self._server = server
        self._storage = Storage()
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
                self._storage[key] = StorageValue(expired_time, message.parsed[2])
                return [SimpleString("OK")]
            case "get":
                return [self._storage[message.parsed[1]]]
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
                master_offset = self._server.offset
                synced_replicas = self._server.count_synced_replicas(master_offset)
                if num_replicas <= synced_replicas:
                    return [self._server.num_replicas]

                trigger = WaitTrigger.create(num_replicas, master_offset)
                self._server.register_trigger(trigger)

                asyncio.create_task(
                    self._server.propagate(
                        Message.from_parsed(
                            [BulkString("REPLCONF"), BulkString("GETACK"), BulkString("*")]
                        )
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
            case _:
                return [ErrorString("Unknown command")]

    def need_store_connection(self, message: Message) -> bool:
        if not isinstance(message.parsed, list) or len(message.parsed) == 0:
            return False
        command = message.parsed[0].lower()
        return command == "psync"
