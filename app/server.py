import asyncio
from pathlib import Path
from typing import Any

from app.command_handler import RedisCommandHandler
from app.persistence import PersistantStorage
from app.redis_serde import BulkString, Message, RedisSerializer
from app.schemas import PEERNAME, Connection, WaitTrigger

CHUNK_SIZE = 500


class RedisServer:
    def __init__(self, port: int, config: dict[str, str] | None = None) -> None:
        self._port = port
        storage = None
        if config and "dir" in config and "dbfilename" in config:
            storage = PersistantStorage(
                Path(config["dir"]) / config["dbfilename"]
            ).create_storage()
        self._handler = RedisCommandHandler(self, storage)
        self._offset = 0
        self.config = config or {}
        self.handshake_finished = False

    async def serve_forever(self) -> None:
        server = await asyncio.start_server(self.handle_client, "localhost", self._port)
        async with server:
            await server.serve_forever()

    async def handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        connection = Connection.create(reader, writer)
        while True:
            message = await reader.read(CHUNK_SIZE)
            if not message:
                break
            print(f"{connection.peername}: {message!r}")
            await self._receive_message(connection, message)

        writer.close()
        await writer.wait_closed()

    async def _pre_handle_hook(self, connection: Connection, message: Message) -> None:
        return None

    async def _post_handle_hook(self, connection: Connection, message: Message) -> None:
        return None

    async def _receive_message(self, connection: Connection, message: bytes) -> None:
        print("received message", message)
        for parsed_message in Message.from_raw(message):
            print(f"parsed message: {parsed_message}")
            await self._pre_handle_hook(connection, parsed_message)

            raw_responses = await self._handler.handle(parsed_message, connection.peername)

            await self._post_handle_hook(connection, parsed_message)
            print("send messages", raw_responses)
            for raw_out_message in raw_responses:
                out_message = RedisSerializer().serialize(raw_out_message)
                print(out_message)
                connection.writer.write(out_message)
                await connection.writer.drain()

    def _inc_offset(self, offset: int) -> None:
        self._offset += offset

    @property
    def is_master(self) -> bool:
        return False

    @property
    def num_replicas(self) -> int:
        return 0

    @property
    def offset(self) -> int:
        return self._offset


class MasterServer(RedisServer):
    def __init__(self, port: int, config: dict[str, str] | None = None) -> None:
        super().__init__(port, config)
        self._port = port
        self._slave_connections: dict[PEERNAME, Connection] = {}
        self._wait_triggers: list[WaitTrigger] = []

    async def _pre_handle_hook(self, connection, message: Message) -> None:
        if self._handler.need_store_connection(message):
            self._slave_connections[connection.peername] = connection

    @property
    def is_master(self) -> bool:
        return True

    @property
    def num_replicas(self) -> int:
        return len(self._slave_connections)

    async def propagate(self, message: Message) -> None:
        self._inc_offset(message.size)
        to_remove = []
        for peername, connection in self._slave_connections.items():
            try:
                connection.writer.write(message.raw)
                await connection.writer.drain()
            except ConnectionResetError:
                to_remove.append(peername)
        for peername in to_remove:
            del self._slave_connections[peername]

    def store_offset(self, peername: PEERNAME, offset: int) -> None:
        if peername not in self._slave_connections:
            print(f"Connection {peername} not found")
            return
        print(
            f"Connection {peername} offset updated from {self._slave_connections[peername].offset} to {offset}"
        )
        self._slave_connections[peername].offset = offset
        self._check_wait_triggers()

    def count_synced_replicas(self, offset: int) -> int:
        count = 0
        print(
            f"Master offset {offset}, slave offsets {[c.offset for c in self._slave_connections.values()]}"
        )
        for connection in self._slave_connections.values():
            if connection.offset >= offset:
                count += 1
        return count

    def register_trigger(self, trigger: WaitTrigger) -> None:
        self._wait_triggers.append(trigger)

    def _check_wait_triggers(self) -> None:
        for trigger in self._wait_triggers:
            if self.count_synced_replicas(trigger.master_offset) >= trigger.num_replicas:
                trigger.event.set()
                self._wait_triggers.remove(trigger)


class SlaveServer(RedisServer):
    def __init__(
        self, port: int, master_host: str, master_port: int, config: dict[str, str] | None = None
    ) -> None:
        super().__init__(port, config)
        self._master_host = master_host
        self._master_port = master_port

    async def serve_forever(self) -> None:
        await self.connect_master()
        return await super().serve_forever()

    async def connect_master(self) -> None:
        reader, writer = await asyncio.open_connection(self._master_host, self._master_port)
        connection = Connection.create(reader, writer)
        await self._send_request(connection, [BulkString("ping")])
        await self._send_request(
            connection,
            [BulkString("REPLCONF"), BulkString("listening-port"), BulkString(self._port)],
        )
        await self._send_request(
            connection,
            [BulkString("REPLCONF"), BulkString("capa"), BulkString("psync2")],
        )
        await self._send_request(
            connection,
            [BulkString("PSYNC"), BulkString("?"), BulkString("-1")],
        )
        asyncio.create_task(self.handle_client(reader, writer))

    async def _send_request(self, connection: Connection, message: Any) -> Any:
        out_message = RedisSerializer().serialize(message)
        connection.writer.write(out_message)
        await connection.writer.drain()
        raw_response = await connection.reader.read(CHUNK_SIZE)
        print(f"Master raw response: {raw_response}")
        await self._receive_message(connection, raw_response)

    async def _post_handle_hook(self, connection: Connection, message: Message) -> None:
        if self.handshake_finished:
            self._inc_offset(message.size)

        # TODO: refactor this
        if isinstance(message.parsed, str) and message.parsed.startswith("REDIS"):
            self.handshake_finished = True
