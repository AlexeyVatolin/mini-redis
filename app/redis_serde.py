from typing import Any, Generator

from app.schemas import Message


class SimpleString(str): ...


class BulkString(str): ...


class RDBString(bytes): ...


class ErrorString(str): ...


class NullString: ...


class RedisSerializer:
    def serialize(self, message: Any) -> bytes:
        return self._serialize_impl(message)

    def _serialize_impl(self, message: Any) -> bytes:
        if isinstance(message, BulkString):
            return f"${len(message)}\r\n{message}\r\n".encode()
        elif isinstance(message, SimpleString):
            return f"+{message}\r\n".encode()
        elif isinstance(message, ErrorString):
            return f"-{message}\r\n".encode()
        elif isinstance(message, RDBString):
            return f"${len(message)}\r\n".encode() + message
        elif isinstance(message, NullString):
            return "$-1\r\n".encode()
        elif isinstance(message, list):
            return f"*{len(message)}\r\n".encode() + b"".join(
                self._serialize_impl(item) for item in message
            )
        else:
            raise ValueError(f"Unsupported message type {type(message)}")


class RedisDeserializer:
    def deserialize(self, message: bytes) -> Generator[Message, None, None]:
        start_index = 0
        while True:
            value, end_index = self._deserialize_impl(message, start_index)
            yield Message(value, end_index - start_index)
            start_index = end_index
            if start_index >= len(message):
                break

    def _deserialize_impl(self, message: str, start_index: int = 0) -> Any:
        if len(message) - start_index == 0:
            return None, None
        if message[start_index] == ord("*"):
            num_elements, end_index = self._parse_number(message, start_index + 1)
            arr = []
            while len(arr) < num_elements:
                elem, end_index = self._deserialize_impl(message, end_index + 2)
                arr.append(elem)
            return arr, end_index + 2
        elif message[start_index] == ord("$"):
            size, end_index = self._parse_number(message, start_index + 1)
            return BulkString(
                message[end_index + 2 : end_index + 2 + size].decode(errors="ignore")
            ), end_index + 2 + size
        elif message[start_index] == ord("+"):
            end_index = message.index(b"\r\n")
            return SimpleString(
                message[start_index + 1 : end_index].decode(errors="ignore")
            ), end_index + 2
        print(f"Unknown message {message}")
        return None, None

    def _parse_number(self, s: str, start_index: int) -> tuple[int, int]:
        end_index = start_index
        while ord("0") <= s[end_index] <= ord("9"):
            end_index += 1
        return int(s[start_index:end_index]), end_index
        return int(s[start_index:end_index]), end_index
