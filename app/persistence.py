import io
from enum import Enum
from pathlib import Path

from app.storage import Storage, StorageValue


class SpecialEncoding(Enum):
    INT_8 = 0
    INT_16 = 1
    INT_32 = 2


class PersistantStorage:
    def __init__(self, file: Path) -> None:
        self._file = file
        self._storage = Storage()

    def create_storage(self):
        if not self._file.exists():
            return self._storage
        with self._file.open("rb") as f:
            print(f"parsing file {f.read()}")
            f.seek(0)
            self._read_magic_string(f)
            self._read_version(f)
            while True:
                try:
                    self._read_next(f)
                except StopIteration:
                    break
        return self._storage

    def _read_magic_string(self, f: io.BufferedReader):
        assert f.read(5) == b"REDIS"

    def _read_version(self, f: io.BufferedReader):
        f.read(4)

    def _read_next(self, f: io.BufferedReader):
        op_code = f.read(1)
        match int.from_bytes(op_code):
            case 0xFA:
                self._read_auxiliary(f)
            case 0xFB:
                self._read_resize_db(f)
            case 0xFE:
                self._read_db_selector(f)
            case 0xFF:
                self._read_checksum(f)
                raise StopIteration
            case 0x00:
                self._read_pair(f)

    def _read_auxiliary(self, f: io.BufferedReader):
        key = self._read_value(f)
        value = self._read_value(f)
        return key, value

    def _read_resize_db(self, f: io.BufferedReader):
        table_size = self._read_length(f)
        expire_hash_table = self._read_length(f)
        return table_size, expire_hash_table

    def _read_db_selector(self, f: io.BufferedReader):
        db_number = int.from_bytes(f.read(1), "big")
        return db_number

    def _read_pair(self, f: io.BufferedReader):
        key = self._read_value(f)
        value = self._read_value(f)
        self._storage[key] = StorageValue(value)

    def _read_checksum(self, f: io.BufferedReader):
        checksum = int.from_bytes(f.read(8), "big")
        return checksum

    def _read_value(self, f: io.BufferedReader) -> str | int:
        length = self._read_length(f)
        if isinstance(length, SpecialEncoding):
            return self._read_special_encoded_int(f, length)

        return f.read(length).decode("utf-8")

    def _read_special_encoded_int(self, f: io.BufferedReader, le_format: SpecialEncoding) -> int:
        return int.from_bytes(f.read(le_format.value + 1))

    def _read_length(self, f: io.BufferedReader) -> int | SpecialEncoding:
        length = int.from_bytes(f.read(1))
        match length >> 6:
            case 0:
                return length
            case 1:
                return (length << 8) + int.from_bytes(f.read(1))
            case 3:
                return SpecialEncoding(length & 0b00111111)
            case _:
                raise ValueError("Invalid length")
