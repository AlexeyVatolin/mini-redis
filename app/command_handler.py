from typing import Any

from app.redis_serde import BulkString, SimpleString


class RedisCommandHandler:
    def handle(self, message: Any) -> Any:
        if not isinstance(message, list) or len(message) == 0:
            return ValueError
        if message[0].lower() == "ping":
            return SimpleString("PONG")
        if message[0].lower() == "echo":
            return BulkString(message[1])
