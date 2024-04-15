from typing import Any

from app.redis_serde import BulkString, ErrorString, NullString, SimpleString

STORAGE = {}


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
                STORAGE[message[1]] = message[2]
                return SimpleString("OK")
            case "get":
                if message[1] in STORAGE:
                    return STORAGE[message[1]]
                else:
                    return NullString()
            case _:
                return ErrorString("Unknown command")
