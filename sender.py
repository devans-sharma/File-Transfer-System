import socket
import threading
import os
import time
import networking
from metadata import generate_source_metadata

MOVIES_ROOT = r"D:/movies" # Double check this folder exists!
CHUNK_SIZE = 32 * 1024 * 1024 
TCP_PORT = 5000
UDP_PORT = 5001

def get_my_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except: return "127.0.0.1"

def start_discovery_beacon():
    """Tells the network 'I am the sender' and listens for pings."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    sock.bind(('', UDP_PORT))
    
    while True:
        try:
            # 1. Shout to everyone
            sock.sendto(b"SENDER_HERE", ('255.255.255.255', UDP_PORT))
            
            # 2. Listen for a ping (non-blocking)
            sock.settimeout(0.5)
            try:
                data, addr = sock.recvfrom(1024)
                if data == b"ANY_SENDER_OPEN?":
                    sock.sendto(b"SENDER_HERE", addr)
            except socket.timeout: pass
        except: pass
        time.sleep(2)

def handle_client(conn, addr):
    try:
        conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        while True:
            msg, _ = networking.recv_msg(conn)
            if not msg: break
            t = msg.get("type")

            if t == "METADATA_REQUEST":
                metadata = generate_source_metadata(MOVIES_ROOT, CHUNK_SIZE)
                networking.send_msg(conn, {"type": "METADATA_RESPONSE", "metadata": metadata})

            elif t == "CHUNK_REQUEST":
                # Ensure the path separator matches the OS
                clean_path = msg["path"].replace("/", os.sep)
                full_path = os.path.join(MOVIES_ROOT, clean_path)
                with open(full_path, "rb") as f:
                    f.seek(msg["chunk_index"] * CHUNK_SIZE)
                    data = f.read(CHUNK_SIZE)
                networking.send_msg(conn, {"type": "CHUNK_RESPONSE", "path": msg["path"], "chunk_index": msg["chunk_index"]}, payload=data)

            elif t == "TRANSFER_COMPLETE": break
    finally:
        conn.close()

def run_sender():
    print(f"\n[SENDER] My IP is: {get_my_ip()}")
    print(f"[*] Serving folder: {MOVIES_ROOT}")
    
    threading.Thread(target=start_discovery_beacon, daemon=True).start()
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("0.0.0.0", TCP_PORT))
    server.listen(5)
    print(f"[SENDER] Online. Waiting for Receiver...\n")
    while True:
        conn, addr = server.accept()
        print(f"[+] Connected to Receiver: {addr}")
        threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()

if __name__ == "__main__":
    run_sender()