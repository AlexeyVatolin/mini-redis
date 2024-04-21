from __future__ import annotations

import base64
import dataclasses
import datetime
from typing import TYPE_CHECKING, Any

from app.redis_serde import BulkString, ErrorString, NullString, RDBString, SimpleString
from app.schemas import Message
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


class RedisCommandHandler:
    def __init__(self, server: RedisServer) -> None:
        self._server = server
        self.master_id = random_id(40) if self._server.is_master else None
        self.offset = 0

    def handle(self, message: Message, from_master: bool = False) -> list[Any]:
        response = self._handle_impl(message)
        if not self._server.is_master and from_master and self._server.handshake_finished:
            self.offset += message.size
            if (
                isinstance(message.parsed, list)
                and len(message.parsed) > 0
                and message.parsed[0].lower() == "replconf"
            ):
                return response
            return []
        if isinstance(message.parsed, str) and message.parsed.startswith("REDIS"):
            self._server.handshake_finished = True
        return response

    def _handle_impl(self, message: Message) -> list[Any]:
        if (
            message.parsed == SimpleString("OK")
            or message.parsed == SimpleString("PONG")
            or isinstance(message.parsed, str)
            and (message.parsed.startswith("REDIS") or message.parsed.startswith("FULLRESYNC"))
        ):
            return []
        if not isinstance(message.parsed, list) or len(message.parsed) == 0:
            return [ErrorString("Wrong message format")]
        command = message.parsed[0].lower()
        match command:
            case "ping":
                return [SimpleString("PONG")]
            case "echo":
                return [BulkString(message.parsed[1])]
            case "set":
                if len(message.parsed) < 3:
                    return [ErrorString("Wrong number of arguments for 'set' command")]
                expired_time = None
                if len(message.parsed) == 5 and message.parsed[3].lower() == "px":
                    expired_time = datetime.datetime.now() + datetime.timedelta(
                        milliseconds=int(message.parsed[4])
                    )
                STORAGE[message.parsed[1]] = StorageValue(expired_time, message.parsed[2])
                return [SimpleString("OK")]
            case "get":
                if message.parsed[1] in STORAGE:
                    if (
                        STORAGE[message.parsed[1]].expired_time is None
                        or STORAGE[message.parsed[1]].expired_time > datetime.datetime.now()
                    ):
                        return [STORAGE[message.parsed[1]].value]
                    else:
                        del STORAGE[message.parsed[1]]

                return [NullString()]
            case "info":
                if len(message.parsed) != 2 or message.parsed[1].lower() != "replication":
                    return [ErrorString("Wrong arguments for 'info' command")]
                role = "master" if self._server.is_master else "slave"
                return [
                    BulkString(
                        f"# Replication\nrole:{role}\nmaster_replid:{self.master_id}\nmaster_repl_offset:{self.offset}"
                    )
                ]
            case "replconf":
                if len(message.parsed) == 3 and message.parsed[1].lower() == "getack":
                    return [
                        [BulkString("REPLCONF"), BulkString("ACK"), BulkString(str(self.offset))]
                    ]

                return [SimpleString("OK")]
            case "psync":
                return [
                    SimpleString(f"FULLRESYNC {self.master_id} {self.offset}"),
                    RDBString(default_rdb),
                ]
            case "wait":
                return [0]
            case _:
                return [ErrorString("Unknown command")]

    def need_propagation(self, message: Message) -> bool:
        if not isinstance(message.parsed, list) or len(message.parsed) == 0:
            return False
        command = message.parsed[0].lower()
        return command in {"set", "del"}

    def need_store_connection(self, message: Message) -> bool:
        if not isinstance(message.parsed, list) or len(message.parsed) == 0:
            return False
        command = message.parsed[0].lower()
        return command == "psync"
