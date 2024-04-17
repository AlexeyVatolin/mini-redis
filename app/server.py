import asyncio
from typing import Any

from app.command_handler import RedisCommandHandler
from app.redis_serde import BulkString, RedisDeserializer, RedisSerializer
from app.utils import random_id

CHUNK_SIZE = 100


class RedisServer:
    def __init__(self, port: int, master_host: str | None, master_port: int | None) -> None:
        self._port = port
        self._master_host = master_host
        self._master_port = master_port
        self._handler = RedisCommandHandler(self)
        self.master_id = random_id(40) if self.is_master else None
        self.offset = 0

        self._slave_connections: list[asyncio.StreamWriter] = []
        self._master_connection = None

    async def connect_master(self) -> None:
        if not self._master_host:
            return
        reader, writer = await asyncio.open_connection(self._master_host, self._master_port)
        await self._send_request(reader, writer, [BulkString("ping")])
        await self._send_request(
            reader,
            writer,
            [BulkString("REPLCONF"), BulkString("listening-port"), BulkString(self._port)],
        )
        await self._send_request(
            reader,
            writer,
            [BulkString("REPLCONF"), BulkString("capa"), BulkString("psync2")],
        )
        await self._send_request(
            reader,
            writer,
            [BulkString("PSYNC"), BulkString("?"), BulkString("-1")],
        )
        await reader.read(CHUNK_SIZE)

        self._master_connection = (reader, writer)

    async def _send_request(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, message: Any
    ) -> Any:
        out_message = RedisSerializer().serialize(message)
        writer.write(out_message)
        await writer.drain()
        raw_response = await reader.read(CHUNK_SIZE)
        print(f"Master raw response: {raw_response}")
        response = RedisDeserializer().deserialize(raw_response)
        print(f"Response from master: {response}")
        return response

    async def handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        addr = writer.get_extra_info("peername")
        while True:
            message = await reader.read(CHUNK_SIZE)
            if not message:
                break
            print(f"{addr}: {message!r}")
            close_connection = await self._receive_message(writer, message)

        if close_connection:
            writer.close()
            await writer.wait_closed()

    async def _receive_message(
        self, writer: asyncio.StreamWriter, message: bytes, send_response: bool = True
    ) -> bool:
        close_connection = True
        print("received message", message)
        parsed_message = RedisDeserializer().deserialize(message)

        if self.is_master and self._handler.need_propagation(parsed_message):
            print("propagate", message)
            await self.propagate(message)

        if self.is_master and self._handler.need_store_connection(parsed_message):
            self._slave_connections.append(writer)
            close_connection = False

        raw_responces = self._handler.handle(parsed_message)
        if send_response:
            for raw_out_message in raw_responces:
                out_message = RedisSerializer().serialize(raw_out_message)
                print(out_message)
                writer.write(out_message)
                await writer.drain()
        return close_connection

    @property
    def is_master(self) -> bool:
        return self._master_host is None

    async def propagate(self, message: Any) -> None:
        for writer in self._slave_connections:
            try:
                writer.write(message)
                await writer.drain()
            except ConnectionResetError:
                self._slave_connections.remove(writer)

    async def wait_master_message(self) -> None:
        if self.is_master:
            return

        while True:
            if self._master_connection is not None:
                message = await self._master_connection[0].read(CHUNK_SIZE)
                if not message:
                    break
                await self._receive_message(
                    self._master_connection[1], message, send_response=False
                )
            await asyncio.sleep(5)
