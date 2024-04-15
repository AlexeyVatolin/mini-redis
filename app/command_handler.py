import dataclasses
import datetime
from typing import Any

from app.redis_serde import BulkString, ErrorString, NullString, SimpleString
from app.utils import random_id


@dataclasses.dataclass
class StorageValue:
    expired_time: datetime.datetime | None
    value: Any


STORAGE: dict[str, StorageValue] = {}


class RedisCommandHandler:
    def __init__(self, is_master: bool) -> None:
        self._is_master = is_master
        self._master_id = random_id(40) if self._is_master else None
        self._offset = 0

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
                role = "master" if self._is_master else "slave"
                return BulkString(
                    f"# Replication\nrole:{role}\nmaster_replid:{self._master_id}\nmaster_repl_offset:{self._offset}"
                )
            case _:
                return ErrorString("Unknown command")
