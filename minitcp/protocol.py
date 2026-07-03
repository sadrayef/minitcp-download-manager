"""
minitcp.protocol
=================

Defines the wire format of MiniTCP packets and helper functions to
build / parse them.

Packet layout (network byte order, big-endian)
-----------------------------------------------

    0                8               16              24              32
    +---------------+---------------+---------------+---------------+
    |                          Checksum (CRC32)                     |
    +---------------+---------------+---------------+---------------+
    |                        Sequence Number (SEQ)                  |
    +---------------+---------------+---------------+---------------+
    |                     Acknowledgement Number (ACK)               |
    +---------------+---------------+---------------+---------------+
    |     Flags     |          Payload Length         |
    +---------------+---------------+---------------+---------------+
    |                            Payload ...                        |
    +---------------------------------------------------------------+

    Checksum : 4 bytes  - CRC32 over (header-without-checksum + payload)
    SEQ      : 4 bytes  - sequence number of THIS packet
    ACK      : 4 bytes  - number being acknowledged (valid when ACK flag set)
    Flags    : 1 byte   - bitmask, see below
    Length   : 2 bytes  - length of payload in bytes
    Payload  : variable - up to MAX_PAYLOAD bytes of application data
"""

import struct
import zlib

# ----------------------------------------------------------------------
# Flags
# ----------------------------------------------------------------------
SYN = 0x01      # connection request  (handshake step 1)
ACK = 0x02      # acknowledgement
FIN = 0x04      # connection / stream termination
DATA = 0x08     # payload carries application data
REQ = 0x10      # application level "request" (e.g. ask for a chunk)
ERR = 0x20      # error notification

FLAG_NAMES = {
    SYN: "SYN", ACK: "ACK", FIN: "FIN", DATA: "DATA", REQ: "REQ", ERR: "ERR"
}


def flags_to_str(flags: int) -> str:
    names = [name for bit, name in FLAG_NAMES.items() if flags & bit]
    return "|".join(names) if names else "-"


# ----------------------------------------------------------------------
# Wire format constants
# ----------------------------------------------------------------------
_HEADER_STRUCT = struct.Struct("!IIBH")   # seq, ack, flags, payload_len
_CHECKSUM_STRUCT = struct.Struct("!I")

HEADER_SIZE = _CHECKSUM_STRUCT.size + _HEADER_STRUCT.size   # 4 + 11 = 15 bytes

# Keep well below typical Ethernet MTU (1500) once header + IP/UDP
# overhead is added, so we never rely on IP fragmentation.
MAX_PAYLOAD = 1024
MAX_PACKET_SIZE = HEADER_SIZE + MAX_PAYLOAD


class PacketError(Exception):
    """Raised when a received datagram cannot be parsed / is corrupt."""


def make_packet(seq: int, ack: int, flags: int, payload: bytes = b"") -> bytes:
    if len(payload) > MAX_PAYLOAD:
        raise ValueError(f"payload too large ({len(payload)} > {MAX_PAYLOAD})")
    header_wo_checksum = _HEADER_STRUCT.pack(seq & 0xFFFFFFFF, ack & 0xFFFFFFFF,
                                              flags & 0xFF, len(payload))
    checksum = zlib.crc32(header_wo_checksum + payload) & 0xFFFFFFFF
    return _CHECKSUM_STRUCT.pack(checksum) + header_wo_checksum + payload


def parse_packet(data: bytes) -> dict:
    if len(data) < HEADER_SIZE:
        raise PacketError("datagram shorter than header")

    (checksum,) = _CHECKSUM_STRUCT.unpack(data[:4])
    header = data[4:4 + _HEADER_STRUCT.size]
    seq, ack, flags, plen = _HEADER_STRUCT.unpack(header)
    payload = data[4 + _HEADER_STRUCT.size: 4 + _HEADER_STRUCT.size + plen]

    if len(payload) != plen:
        raise PacketError("truncated payload")

    calc = zlib.crc32(header + payload) & 0xFFFFFFFF
    if calc != checksum:
        raise PacketError("checksum mismatch (corrupted packet)")

    return {"seq": seq, "ack": ack, "flags": flags, "payload": payload}


def describe(pkt: dict) -> str:
    return (f"[{flags_to_str(pkt['flags']):<12}] seq={pkt['seq']:<10} "
            f"ack={pkt['ack']:<10} len={len(pkt['payload'])}")
