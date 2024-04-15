from typing import Any


class SimpleString(str): ...


class BulkString(str): ...


class RedisSerializer:
    def serialize(self, message: Any) -> bytes:
        return self._serialize_impl(message).encode()

    def _serialize_impl(self, message: Any) -> str:
        if isinstance(message, BulkString):
            return f"${len(message)}\r\n{message}\r\n"
        elif isinstance(message, SimpleString):
            return f"+{message}\r\n"


class RedisDeserializer:
    def deserialize(self, message: bytes) -> Any:
        return self._deserialize_impl(message.decode())

    def _deserialize_impl(self, message: str, start_index: int = 0) -> Any:
        if message[start_index] == "*":
            num_elements, end_index = self._parse_number(message, start_index + 1)
            arr = []
            while len(arr) < num_elements:
                elem, end_index = self._deserialize_impl(message, end_index + 2)
                arr.append(elem)
            return arr
        elif message[start_index] == "$":
            size, end_index = self._parse_number(message, start_index + 1)
            return BulkString(message[end_index + 2 : end_index + 2 + size]), end_index + 2 + size

    def _parse_number(self, s: str, start_index: int) -> tuple[int, int]:
        end_index = start_index
        while s[end_index].isdigit():
            end_index += 1
        return int(s[start_index:end_index]), end_index
