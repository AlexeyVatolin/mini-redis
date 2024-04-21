import argparse
import asyncio

from app.server import RedisServer


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
    redis_server = RedisServer(args.port, master_host, master_port)

    await redis_server.connect_master()
    server = await asyncio.start_server(redis_server.handle_client, "localhost", args.port)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
