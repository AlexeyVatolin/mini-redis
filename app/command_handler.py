import dataclasses
import datetime
from typing import Any

from app.redis_serde import BulkString, ErrorString, NullString, SimpleString


@dataclasses.dataclass
class StorageValue:
    expired_time: datetime.datetime | None
    value: Any


STORAGE: dict[str, StorageValue] = {}


class RedisCommandHandler:
    def handle(self, message: Any) -> Any:
        if not isinstance(message, list) or len(message) == 0:
            return ValueError
        command = message[0].lower()
        match command:
            case "ping":
                return SimpleString("PONG")
            case "echo":
                return BulkString(message[1])
            case "set":
                if len(message) < 3:
                    return ErrorString("Wrong number of arguments for 'set' command")
                expired_time = None
                if len(message) == 5 and message[3].lower() == "px":
                    expired_time = datetime.datetime.now() + datetime.timedelta(
                        milliseconds=int(message[4])
                    )
                STORAGE[message[1]] = StorageValue(expired_time, message[2])
                return SimpleString("OK")
            case "get":
                if message[1] in STORAGE:
                    if (
                        STORAGE[message[1]].expired_time is None
                        or STORAGE[message[1]].expired_time > datetime.datetime.now()
                    ):
                        return STORAGE[message[1]].value
                    else:
                        del STORAGE[message[1]]

                return NullString()
            case "info":
                if len(message) != 2 or message[1].lower() != "replication":
                    return ErrorString("Wrong arguments for 'info' command")
                return BulkString("# Replication\nrole:master")
            case _:
                return ErrorString("Unknown command")
