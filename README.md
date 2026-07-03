## MiniTCP Protocol (summary)

MiniTCP is a reliable transport protocol built on UDP:

- **3-way handshake** — SYN → SYN|ACK → ACK, with each connection
  getting its own ephemeral UDP port (TFTP-style), so multiple
  connections run fully independently.
- **Sequence numbers + ACKs** — every data packet is numbered and
  acknowledged individually (stop-and-wait).
- **Timeout & retransmission** — unacknowledged packets are resent
  automatically with exponential backoff.
- **Checksums** — CRC32 on every packet detects corruption.
- **Connection termination** — FIN/ACK based close.

Full packet format and design rationale: [protocol_design.pdf](docs/protocol_design.pdf)
