"""
minitcp.connection
===================

Implements the actual MiniTCP behaviour on top of a UDP socket:

  * 3-way handshake            (connect / accept)
  * sequence numbers per packet
  * cumulative ACKs
  * timeout based retransmission (stop-and-wait ARQ)
  * 4-way connection termination
  * optional artificial packet loss (for demoing reliability)

Design note on "independent connections" over UDP
---------------------------------------------------
A single UDP socket has no notion of a "connected" peer the way TCP
does. To let a MiniTCP *server* accept many independent, concurrently
managed connections (as required by the Download Manager, which opens
9 parallel connections) we use the same trick TFTP uses:

  1. The server listens for SYN packets on one well-known port.
  2. For every new SYN it receives, it creates a *brand new* UDP
     socket bound to an ephemeral (OS assigned) port and spawns a
     worker thread that owns that socket exclusively.
  3. The SYN-ACK is sent back from that new ephemeral port, so the
     client learns it and all further traffic for that logical
     connection goes over the new (client_port <-> ephemeral_port)
     pair, isolated from every other connection.

This gives every MiniTCP connection its own private UDP "pipe",
its own sequence-number space, its own retransmission timers, and
lets many connections be serviced fully in parallel (one thread each)
without interfering with one another.
"""

import socket
import time
import random
import threading

from . import protocol as p

# ----------------------------------------------------------------------
# Tunable protocol parameters
# ----------------------------------------------------------------------
DEFAULT_TIMEOUT = 0.4          # seconds before a packet is considered lost
MAX_RETRIES = 12               # retransmission attempts before giving up
HANDSHAKE_TIMEOUT = 1.0
HANDSHAKE_RETRIES = 10


class MiniTCPError(Exception):
    pass


class ConnectionClosed(MiniTCPError):
    pass


class MiniTCPConnection:
    """
    One established MiniTCP connection (client OR server side).
    Wraps a UDP socket that is `connect()`-ed to a single remote
    peer, so `send`/`recv` on it only exchange datagrams with that
    peer.
    """

    def __init__(self, sock: socket.socket, remote_addr, is_server: bool,
                 loss_rate: float = 0.0, verbose: bool = False):
        self.sock = sock
        self.remote_addr = remote_addr
        self.is_server = is_server
        self.loss_rate = loss_rate     # probability [0,1] of DROPPING an outgoing packet (test tool)
        self.verbose = verbose

        self.local_seq = random.randint(1, 10_000)   # next seq number *we* will use
        self.remote_seq = 0                           # last seq number *we* have accepted from peer
        self._closed = False
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # low level helpers
    # ------------------------------------------------------------------
    def _log(self, msg):
        if self.verbose:
            tag = "SERVER" if self.is_server else "CLIENT"
            print(f"[{tag} {self.remote_addr}] {msg}")

    def _raw_send(self, flags, seq, ack, payload=b""):
        if self.loss_rate and random.random() < self.loss_rate:
            self._log(f"(simulated loss) DROPPED -> {p.flags_to_str(flags)} seq={seq}")
            return
        pkt = p.make_packet(seq, ack, flags, payload)
        self.sock.sendto(pkt, self.remote_addr)
        self._log(f"SEND {p.describe({'seq': seq, 'ack': ack, 'flags': flags, 'payload': payload})}")

    def _raw_recv(self, timeout):
        self.sock.settimeout(timeout)
        try:
            data, addr = self.sock.recvfrom(p.MAX_PACKET_SIZE)
        except socket.timeout:
            return None
        try:
            pkt = p.parse_packet(data)
        except p.PacketError:
            self._log("received corrupt packet, ignoring")
            return None
        self._log(f"RECV {p.describe(pkt)}")
        return pkt

    # ------------------------------------------------------------------
    # reliable single-packet-with-ACK primitive (stop-and-wait ARQ)
    # ------------------------------------------------------------------
    def _send_and_wait_ack(self, flags, seq, ack, payload=b"", expect_ack_of=None):
        """
        Sends one packet and blocks until the matching ACK arrives,
        retransmitting on timeout. Returns the ACK packet.
        Raises MiniTCPError after MAX_RETRIES failed attempts.
        """
        expect_ack_of = seq if expect_ack_of is None else expect_ack_of
        timeout = DEFAULT_TIMEOUT
        for attempt in range(1, MAX_RETRIES + 1):
            self._raw_send(flags, seq, ack, payload)
            pkt = self._raw_recv(timeout)
            if pkt is not None and (pkt["flags"] & p.ACK) and pkt["ack"] == expect_ack_of:
                return pkt
            self._log(f"timeout/mismatch, retransmitting (attempt {attempt})")
            timeout = min(timeout * 1.5, 3.0)   # simple backoff
        raise MiniTCPError("max retries exceeded, peer unreachable")

    # ------------------------------------------------------------------
    # Reliable stream send / receive (used by the Download Manager)
    # ------------------------------------------------------------------
    def send(self, data: bytes):
        """
        Reliably transmits `data` to the peer, chunking it into
        MAX_PAYLOAD-sized DATA packets, each individually
        sequenced and acknowledged before the next is sent
        (stop-and-wait), with timeout-based retransmission.
        Finishes with a FIN so the receiver knows the stream ended.
        """
        offset = 0
        total = len(data)
        while offset < total:
            chunk = data[offset: offset + p.MAX_PAYLOAD]
            seq = self.local_seq
            self._send_and_wait_ack(p.DATA, seq, 0, chunk, expect_ack_of=seq)
            self.local_seq += 1
            offset += len(chunk)
        # signal end of stream
        self._send_and_wait_ack(p.FIN, self.local_seq, 0, b"", expect_ack_of=self.local_seq)
        self.local_seq += 1

    def recv(self) -> bytes:
        """
        Reliably receives a full stream sent via `send()`, returning
        the reassembled bytes. ACKs every in-order DATA packet and
        discards/re-ACKs duplicates (in case an ACK was itself lost
        and the sender retransmitted).
        """
        buf = bytearray()
        expected_seq = None
        while True:
            pkt = self._raw_recv(DEFAULT_TIMEOUT * 4)
            if pkt is None:
                continue  # keep waiting; sender will retransmit if needed
            if expected_seq is None:
                expected_seq = pkt["seq"]

            if pkt["flags"] & p.FIN:
                # ack the FIN and stop
                self._raw_send(p.ACK, 0, pkt["seq"])
                break

            if pkt["flags"] & p.DATA:
                if pkt["seq"] == expected_seq:
                    buf.extend(pkt["payload"])
                    self._raw_send(p.ACK, 0, pkt["seq"])
                    expected_seq += 1
                elif pkt["seq"] < expected_seq:
                    # duplicate (our previous ACK was probably lost) - re-ACK it
                    self._raw_send(p.ACK, 0, pkt["seq"])
                else:
                    # out of order / gap - ignore, sender will time out and resend
                    self._log(f"out-of-order packet seq={pkt['seq']} expected={expected_seq}, dropped")
        return bytes(buf)

    # ------------------------------------------------------------------
    # small request/response helpers (application layer control msgs)
    # ------------------------------------------------------------------
    def send_request(self, payload: bytes):
        seq = self.local_seq
        self._send_and_wait_ack(p.REQ, seq, 0, payload, expect_ack_of=seq)
        self.local_seq += 1

    def recv_request(self, timeout=5.0) -> bytes:
        pkt = self._raw_recv(timeout)
        if pkt is None or not (pkt["flags"] & p.REQ):
            raise MiniTCPError("expected REQ packet, got none/other")
        self._raw_send(p.ACK, 0, pkt["seq"])
        return pkt["payload"]

    # ------------------------------------------------------------------
    # Connection termination (4-way close, like TCP FIN/ACK/FIN/ACK)
    # ------------------------------------------------------------------
    def close_active(self):
        """Initiate termination: send FIN, wait ACK, wait peer FIN, ACK it."""
        if self._closed:
            return
        seq = self.local_seq
        self._send_and_wait_ack(p.FIN, seq, 0, b"", expect_ack_of=seq)
        self.local_seq += 1
        # wait for peer's own FIN
        for _ in range(MAX_RETRIES):
            pkt = self._raw_recv(DEFAULT_TIMEOUT)
            if pkt and (pkt["flags"] & p.FIN):
                self._raw_send(p.ACK, 0, pkt["seq"])
                break
        self._closed = True
        self._log("connection closed (active)")

    def close_passive(self):
        """Respond to peer-initiated termination it already sent (see recv())."""
        if self._closed:
            return
        seq = self.local_seq
        self._send_and_wait_ack(p.FIN, seq, 0, b"", expect_ack_of=seq)
        self.local_seq += 1
        self._closed = True
        self._log("connection closed (passive)")

    def shutdown(self):
        try:
            self.sock.close()
        except OSError:
            pass


# ==========================================================================
# Client side: connect()
# ==========================================================================
def connect(server_ip: str, server_port: int, timeout=HANDSHAKE_TIMEOUT,
            retries=HANDSHAKE_RETRIES, loss_rate=0.0, verbose=False) -> MiniTCPConnection:
    """
    Performs the client side of the MiniTCP 3-way handshake:

        Client -> Server : SYN(seq=x)
        Server -> Client : SYN|ACK(seq=y, ack=x+1)   [from a NEW ephemeral port]
        Client -> Server : ACK(ack=y+1)               [to that new port]

    Returns a connected MiniTCPConnection bound to the server's
    per-connection ephemeral port.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("", 0))  # ephemeral local port

    x = random.randint(1, 10_000)
    syn_pkt = p.make_packet(x, 0, p.SYN)

    server_addr = (server_ip, server_port)
    sock.settimeout(timeout)

    for attempt in range(1, retries + 1):
        sock.sendto(syn_pkt, server_addr)
        try:
            data, addr = sock.recvfrom(p.MAX_PACKET_SIZE)
        except socket.timeout:
            continue
        try:
            pkt = p.parse_packet(data)
        except p.PacketError:
            continue
        if (pkt["flags"] & p.SYN) and (pkt["flags"] & p.ACK) and pkt["ack"] == x + 1:
            # got SYN-ACK from server's new per-connection port `addr`
            y = pkt["seq"]
            ack_pkt = p.make_packet(x + 1, y + 1, p.ACK)
            sock.sendto(ack_pkt, addr)
            conn = MiniTCPConnection(sock, addr, is_server=False,
                                      loss_rate=loss_rate, verbose=verbose)
            conn.local_seq = x + 1
            conn.remote_seq = y + 1
            return conn

    sock.close()
    raise MiniTCPError(f"handshake failed after {retries} attempts (server unreachable?)")


# ==========================================================================
# Server side: listen loop + accept
# ==========================================================================
def serve_forever(listen_port: int, handler, loss_rate=0.0, verbose=False):
    """
    Runs a MiniTCP server: listens for SYN packets on `listen_port`
    and, for each new client, spawns a thread that completes the
    handshake on a fresh ephemeral socket and calls
    `handler(connection)`. Multiple connections are therefore
    serviced fully concurrently.
    """
    listen_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    listen_sock.bind(("", listen_port))
    print(f"[MiniTCP server] listening for connections on UDP port {listen_port}")

    while True:
        data, client_addr = listen_sock.recvfrom(p.MAX_PACKET_SIZE)
        try:
            pkt = p.parse_packet(data)
        except p.PacketError:
            continue
        if not (pkt["flags"] & p.SYN):
            continue  # ignore stray non-SYN traffic on the well-known port

        t = threading.Thread(
            target=_accept_worker,
            args=(client_addr, pkt["seq"], handler, loss_rate, verbose),
            daemon=True,
        )
        t.start()


def _accept_worker(client_addr, client_x, handler, loss_rate, verbose):
    """Completes the handshake on a brand-new ephemeral socket, then hands
    the resulting connection to the application-supplied handler."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("", 0))  # OS-assigned ephemeral port -> makes this connection independent

    y = random.randint(1, 10_000)
    synack = p.make_packet(y, client_x + 1, p.SYN | p.ACK)

    sock.settimeout(HANDSHAKE_TIMEOUT)
    for attempt in range(1, HANDSHAKE_RETRIES + 1):
        sock.sendto(synack, client_addr)
        try:
            data, addr = sock.recvfrom(p.MAX_PACKET_SIZE)
        except socket.timeout:
            continue
        if addr[0] != client_addr[0]:
            continue
        try:
            pkt = p.parse_packet(data)
        except p.PacketError:
            continue
        if (pkt["flags"] & p.ACK) and pkt["ack"] == y + 1:
            conn = MiniTCPConnection(sock, client_addr, is_server=True,
                                      loss_rate=loss_rate, verbose=verbose)
            conn.local_seq = y + 1
            conn.remote_seq = client_x + 1
            try:
                handler(conn)
            finally:
                conn.shutdown()
            return

    sock.close()  # handshake never completed, give up on this client
