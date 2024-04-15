import argparse
import asyncio
from typing import Any

from app.command_handler import RedisCommandHandler
from app.redis_serde import BulkString, RedisDeserializer, RedisSerializer

CHUNK_SIZE = 100


class RedisServer:
    def __init__(self, port: int, master_host: str | None, master_port: int | None) -> None:
        self._port = port
        self._master_host = master_host
        self._master_port = master_port
        self._handler = RedisCommandHandler(self._master_host is None)

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

            parsed_message = RedisDeserializer().deserialize(message)
            out_message = RedisSerializer().serialize(self._handler.handle(parsed_message))
            print(out_message)
            writer.write(out_message)
            await writer.drain()

        writer.close()
        await writer.wait_closed()


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--replicaof", type=str, nargs="+", default=None)
    args = parser.parse_args()

    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")
    master_host, master_port = None, None
    if args.replicaof:
        master_host, master_port = args.replicaof
    context = RedisServer(args.port, master_host, master_port)
    await context.connect_master()
    server = await asyncio.start_server(context.handle_client, "localhost", args.port)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
