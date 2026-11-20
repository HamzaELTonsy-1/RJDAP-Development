import json
import struct
import zlib

MAGIC = b"RJ"
VERSION = 1

WORKER_JOIN = 1
HEARTBEAT = 2
TASK_DISPATCH = 3
TASK_RESULT = 4
ACK_RESULT = 5
JOIN_ACK = 6

# 30-byte RJDAP header
HEADER = struct.Struct("!2sBBBBIHHHHIII")
HEADER_SIZE = HEADER.size

MAX_DATAGRAM = 512
MAX_PAYLOAD = MAX_DATAGRAM - HEADER_SIZE


def encode_message(
    message_type,
    task_id=0,
    worker_id=0,
    attempt_num=0,
    generation=0,
    receipt_tag=0,
    sequence_num=0,
    payload=None,
    flags=0,
):
    if payload is None:
        payload = {}

    payload_bytes = json.dumps(payload).encode("utf-8")

    if len(payload_bytes) > MAX_PAYLOAD:
        raise ValueError("Payload exceeds 482 bytes")

    checksum = zlib.crc32(payload_bytes) & 0xFFFFFFFF

    header = HEADER.pack(
        MAGIC,
        VERSION,
        message_type,
        flags,
        HEADER_SIZE,
        task_id,
        worker_id,
        attempt_num,
        generation,
        len(payload_bytes),
        checksum,
        sequence_num,
        receipt_tag,
    )

    return header + payload_bytes


def decode_message(data):
    if len(data) < HEADER_SIZE:
        raise ValueError("Packet is shorter than the protocol header")

    (
        magic,
        version,
        message_type,
        flags,
        header_len,
        task_id,
        worker_id,
        attempt_num,
        generation,
        payload_len,
        checksum,
        sequence_num,
        receipt_tag,
    ) = HEADER.unpack(data[:HEADER_SIZE])

    if magic != MAGIC:
        raise ValueError("Invalid protocol magic")

    if version != VERSION:
        raise ValueError("Unsupported protocol version")

    if header_len != HEADER_SIZE:
        raise ValueError("Invalid header length")

    if payload_len > MAX_PAYLOAD:
        raise ValueError("Payload exceeds 482 bytes")

    payload_bytes = data[HEADER_SIZE:]

    if len(payload_bytes) != payload_len:
        raise ValueError("Payload length does not match header")

    actual_checksum = zlib.crc32(payload_bytes) & 0xFFFFFFFF

    if actual_checksum != checksum:
        raise ValueError("Payload checksum mismatch")

    payload = json.loads(payload_bytes.decode("utf-8"))

    return {
        "message_type": message_type,
        "flags": flags,
        "task_id": task_id,
        "worker_id": worker_id,
        "attempt_num": attempt_num,
        "generation": generation,
        "receipt_tag": receipt_tag,
        "sequence_num": sequence_num,
        "payload": payload,
    }
