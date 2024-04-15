import argparse
import asyncio

from app.command_handler import RedisCommandHandler
from app.redis_serde import RedisDeserializer, RedisSerializer

CHUNK_SIZE = 100


class ClientContext:
    def __init__(self, master_host: str | None, master_port: int | None) -> None:
        self._master_host = master_host
        self._master_port = master_port

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
            handler = RedisCommandHandler(self._master_host is None)
            out_message = RedisSerializer().serialize(handler.handle(parsed_message))
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
    context = ClientContext(master_host, master_port)
    server = await asyncio.start_server(context.handle_client, "localhost", args.port)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
