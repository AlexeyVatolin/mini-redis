import asyncio

from app.command_handler import RedisCommandHandler
from app.redis_serde import RedisDeserializer, RedisSerializer

CHUNK_SIZE = 100


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    addr = writer.get_extra_info("peername")
    while True:
        message = await reader.read(CHUNK_SIZE)
        if not message:
            break
        print(f"{addr}: {message!r}")

        parsed_message = RedisDeserializer().deserialize(message)
        handler = RedisCommandHandler()
        out_message = RedisSerializer().serialize(handler.handle(parsed_message))
        print(out_message)
        writer.write(out_message)
        await writer.drain()

    writer.close()
    await writer.wait_closed()


async def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")
    server = await asyncio.start_server(handle_client, "localhost", 6379)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
