#!/usr/bin/env python3
"""
client.py - Download Manager (Part 2 of the project)

  1. Opens one MiniTCP connection to ask the server for file info
     (name, size, number of chunks).
  2. Opens `num_chunks` INDEPENDENT MiniTCP connections in parallel
     (one thread each) and downloads one chunk per connection.
  3. All chunks are downloaded and managed concurrently; each is
     buffered in memory (or a temp file) as it completes.
  4. Once every chunk has arrived, the client reassembles them, in
     order, into the final output file.

Usage:
    python3 client.py --host 127.0.0.1 --port 9000 --out downloaded.bin
    python3 client.py --host 127.0.0.1 --port 9000 --out downloaded.bin --loss 0.05 -v
"""

import argparse
import json
import os
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from minitcp import connect, MiniTCPError


def fetch_info(host, port, loss_rate, verbose):
    conn = connect(host, port, loss_rate=loss_rate, verbose=verbose)
    try:
        conn.send_request(json.dumps({"type": "info"}).encode("utf-8"))
        raw = conn.recv()
        return json.loads(raw.decode("utf-8"))
    finally:
        conn.shutdown()


def fetch_chunk(host, port, index, loss_rate, verbose, results, errors):
    try:
        conn = connect(host, port, loss_rate=loss_rate, verbose=verbose)
        try:
            conn.send_request(json.dumps({"type": "chunk", "index": index}).encode("utf-8"))
            data = conn.recv()
            results[index] = data
            print(f"[client] chunk {index} downloaded ({len(data)} bytes) "
                  f"over independent connection")
        finally:
            conn.shutdown()
    except MiniTCPError as e:
        errors[index] = str(e)


def main():
    ap = argparse.ArgumentParser(description="MiniTCP Download Manager - client")
    ap.add_argument("--host", required=True, help="server IP/hostname")
    ap.add_argument("--port", type=int, default=9000, help="server UDP port")
    ap.add_argument("--out", required=True, help="path to write the reassembled file to")
    ap.add_argument("--loss", type=float, default=0.0,
                     help="probability (0-1) of dropping an outgoing packet "
                          "(client-side loss simulation)")
    ap.add_argument("-v", "--verbose", action="store_true", help="print every packet sent/received")
    args = ap.parse_args()

    print(f"[client] requesting file info from {args.host}:{args.port} ...")
    info = fetch_info(args.host, args.port, args.loss, args.verbose)
    num_chunks = info["num_chunks"]
    print(f"[client] file: {info['filename']}  size: {info['size']} bytes  "
          f"chunks: {num_chunks}")

    results = {}
    errors = {}
    threads = []

    start = time.time()
    print(f"[client] opening {num_chunks} independent MiniTCP connections in parallel ...")
    for i in range(num_chunks):
        t = threading.Thread(
            target=fetch_chunk,
            args=(args.host, args.port, i, args.loss, args.verbose, results, errors),
        )
        threads.append(t)
        t.start()

    for t in threads:
        t.join()
    elapsed = time.time() - start

    if errors:
        print("[client] the following chunks FAILED to download:")
        for idx, err in errors.items():
            print(f"    chunk {idx}: {err}")
        sys.exit(1)

    missing = [i for i in range(num_chunks) if i not in results]
    if missing:
        print(f"[client] missing chunks: {missing}")
        sys.exit(1)

    print(f"[client] all {num_chunks} chunks received in {elapsed:.2f}s, reassembling file ...")
    with open(args.out, "wb") as f:
        for i in range(num_chunks):
            f.write(results[i])

    downloaded_size = os.path.getsize(args.out)
    ok = downloaded_size == info["size"]
    print(f"[client] wrote '{args.out}' ({downloaded_size} bytes) "
          f"- {'OK, size matches' if ok else 'SIZE MISMATCH!'}")


if __name__ == "__main__":
    main()
