#!/usr/bin/env python3
"""
server.py - Download Manager file server (Part 2 of the project)

Serves ONE fixed file over MiniTCP. The file is logically split into
`--chunks` (default 9) roughly equal byte ranges. Each connecting
client is handled on its own thread / its own MiniTCP connection
(see minitcp.connection.serve_forever), so many chunk requests -
e.g. the 9 parallel requests issued by client.py - are served fully
concurrently.

Usage:
    python3 server.py --file testfiles/sample.bin --port 9000 --chunks 9
    python3 server.py --file testfiles/sample.bin --port 9000 --loss 0.05 -v
                                                    (simulate 5% packet loss)
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from minitcp import serve_forever, MiniTCPError
from minitcp import protocol as p


def chunk_bounds(file_size: int, num_chunks: int, index: int):
    """Byte range [start, end) for chunk `index` of `num_chunks`."""
    base = file_size // num_chunks
    remainder = file_size % num_chunks
    start = index * base + min(index, remainder)
    size = base + (1 if index < remainder else 0)
    return start, start + size


def make_handler(filepath: str, num_chunks: int):
    file_size = os.path.getsize(filepath)
    filename = os.path.basename(filepath)

    def handler(conn):
        peer = conn.remote_addr
        try:
            raw_req = conn.recv_request()
            req = json.loads(raw_req.decode("utf-8"))
        except (MiniTCPError, json.JSONDecodeError, UnicodeDecodeError) as e:
            print(f"[server] bad request from {peer}: {e}")
            return

        if req.get("type") == "info":
            resp = {
                "filename": filename,
                "size": file_size,
                "num_chunks": num_chunks,
            }
            conn.send(json.dumps(resp).encode("utf-8"))
            print(f"[server] {peer} <- file info ({filename}, {file_size} bytes, {num_chunks} chunks)")

        elif req.get("type") == "chunk":
            index = int(req["index"])
            if not (0 <= index < num_chunks):
                print(f"[server] {peer} requested invalid chunk index {index}")
                return
            start, end = chunk_bounds(file_size, num_chunks, index)
            with open(filepath, "rb") as f:
                f.seek(start)
                data = f.read(end - start)
            conn.send(data)
            print(f"[server] {peer} <- chunk {index} "
                  f"[{start}:{end}] ({len(data)} bytes) sent")
        else:
            print(f"[server] {peer} sent unknown request type: {req}")

    return handler


def main():
    ap = argparse.ArgumentParser(description="MiniTCP Download Manager - file server")
    ap.add_argument("--file", required=True, help="path of the file to serve")
    ap.add_argument("--port", type=int, default=9000, help="UDP well-known port to listen on")
    ap.add_argument("--chunks", type=int, default=9, help="number of chunks to split the file into")
    ap.add_argument("--loss", type=float, default=0.0,
                     help="probability (0-1) of dropping an outgoing packet, "
                          "used to demonstrate retransmission/reliability")
    ap.add_argument("-v", "--verbose", action="store_true", help="print every packet sent/received")
    args = ap.parse_args()

    if not os.path.isfile(args.file):
        print(f"error: file not found: {args.file}")
        sys.exit(1)

    print(f"[server] serving '{args.file}' "
          f"({os.path.getsize(args.file)} bytes) split into {args.chunks} chunks")
    if args.loss > 0:
        print(f"[server] simulated packet loss rate: {args.loss:.0%}")

    handler = make_handler(args.file, args.chunks)
    try:
        serve_forever(args.port, handler, loss_rate=args.loss, verbose=args.verbose)
    except KeyboardInterrupt:
        print("\n[server] shutting down")


if __name__ == "__main__":
    main()
