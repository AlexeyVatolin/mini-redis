from __future__ import annotations

import base64
import dataclasses
import datetime
from typing import TYPE_CHECKING, Any

from app.redis_serde import BulkString, ErrorString, NullString, RDBString, SimpleString

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

    def handle(self, message: Any) -> list[Any]:
        if message == SimpleString("OK"):
            return []
        if not isinstance(message, list) or len(message) == 0:
            return [ErrorString("Wrong message format")]
        command = message[0].lower()
        match command:
            case "ping":
                return [SimpleString("PONG")]
            case "echo":
                return [BulkString(message[1])]
            case "set":
                if len(message) < 3:
                    return [ErrorString("Wrong number of arguments for 'set' command")]
                expired_time = None
                if len(message) == 5 and message[3].lower() == "px":
                    expired_time = datetime.datetime.now() + datetime.timedelta(
                        milliseconds=int(message[4])
                    )
                STORAGE[message[1]] = StorageValue(expired_time, message[2])
                return [SimpleString("OK")]
            case "get":
                if message[1] in STORAGE:
                    if (
                        STORAGE[message[1]].expired_time is None
                        or STORAGE[message[1]].expired_time > datetime.datetime.now()
                    ):
                        return [STORAGE[message[1]].value]
                    else:
                        del STORAGE[message[1]]

                return [NullString()]
            case "info":
                if len(message) != 2 or message[1].lower() != "replication":
                    return [ErrorString("Wrong arguments for 'info' command")]
                role = "master" if self._server.is_master else "slave"
                return [
                    BulkString(
                        f"# Replication\nrole:{role}\nmaster_replid:{self._server.master_id}\nmaster_repl_offset:{self._server.offset}"
                    )
                ]
            case "replconf":
                return [SimpleString("OK")]
            case "psync":
                return [
                    SimpleString(f"FULLRESYNC {self._server.master_id} {self._server.offset}"),
                    RDBString(default_rdb),
                ]
            case _:
                return [ErrorString("Unknown command")]

    def need_propagation(self, message: Any) -> bool:
        if not isinstance(message, list) or len(message) == 0:
            return False
        command = message[0].lower()
        return command in {"set", "del"}

    def need_store_connection(self, message: Any) -> bool:
        if not isinstance(message, list) or len(message) == 0:
            return False
        command = message[0].lower()
        return command == "psync"
