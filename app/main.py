import argparse
import asyncio

from app.server import MasterServer, SlaveServer


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--replicaof", type=str, nargs="+", default=None)
    parser.add_argument("--dir", type=str, default=None)
    parser.add_argument("--dbfilename", type=str, default=None)
    args = parser.parse_args()

    config = {}
    if args.dir:
        config["dir"] = args.dir
    if args.dbfilename:
        config["dbfilename"] = args.dbfilename
    if args.replicaof:
        master_host, master_port = args.replicaof
        server = SlaveServer(args.port, master_host, master_port, config=config)
    else:
        server = MasterServer(args.port, config=config)
    await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
