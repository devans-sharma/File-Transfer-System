import socket
import struct
import json
import hashlib

HEADER_FMT = '!I'
HEADER_SIZE = struct.calcsize(HEADER_FMT)

def calculate_hash(data):
    return hashlib.sha256(data).hexdigest()

def send_msg(sock, msg_dict, payload=b''):
    json_bytes = json.dumps(msg_dict).encode('utf-8')
    full_payload = json_bytes + payload
    msg_len = len(full_payload)
    sock.sendall(struct.pack(HEADER_FMT, msg_len) + full_payload)

def recv_msg(sock):
    raw_len = _recv_n_bytes(sock, HEADER_SIZE)
    if not raw_len:
        return None, None

    msg_len = struct.unpack(HEADER_FMT, raw_len)[0]
    full_payload = _recv_n_bytes(sock, msg_len)
    if not full_payload:
        return None, None

    decoder = json.JSONDecoder()
    content_str = full_payload.decode('utf-8', errors='ignore')
    msg_dict, index = decoder.raw_decode(content_str)

    json_len = len(json.dumps(msg_dict).encode('utf-8'))
    return msg_dict, full_payload[json_len:]


def _recv_n_bytes(sock, n):
    view = memoryview(bytearray(n))
    pos = 0

    CHUNK_READ_SIZE = 64 * 1024  # batched read size (64KB)

    while pos < n:
        remaining = n - pos
        read_size = min(remaining, CHUNK_READ_SIZE)

        read_len = sock.recv_into(view[pos:pos + read_size], read_size)
        if read_len == 0:
            return None

        pos += read_len

    return view.tobytes()
