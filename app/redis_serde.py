from typing import Any


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
    def deserialize(self, message: bytes) -> Any:
        message_str = message.decode(errors="ignore")
        start_index = 0
        while True:
            value, start_index = self._deserialize_impl(message_str, start_index)
            yield value
            if start_index == len(message_str):
                break

    def _deserialize_impl(self, message: str, start_index: int = 0) -> Any:
        if len(message) - start_index == 0:
            return None, None
        if message[start_index] == "*":
            num_elements, end_index = self._parse_number(message, start_index + 1)
            arr = []
            while len(arr) < num_elements:
                elem, end_index = self._deserialize_impl(message, end_index + 2)
                arr.append(elem)
            return arr, end_index + 2
        elif message[start_index] == "$":
            size, end_index = self._parse_number(message, start_index + 1)
            return BulkString(message[end_index + 2 : end_index + 2 + size]), end_index + 2 + size
        elif message[start_index] == "+":
            return SimpleString(message[start_index + 1 : len(message) - 2]), len(message)
        print(f"Unknown message {message}")
        return None, None

    def _parse_number(self, s: str, start_index: int) -> tuple[int, int]:
        end_index = start_index
        while s[end_index].isdigit():
            end_index += 1
        return int(s[start_index:end_index]), end_index


print(
    list(
        RedisDeserializer().deserialize(
            b"+FULLRESYNC 75cd7bc10c49047e0d163660f3b90625b1af31dc 0\r\n$88\r\nREDIS0011\xfa\tredis-ver\x057.2.0\xfa\nredis-bits\xc0"
        )
    )
)
