import socket, os, json, time, networking
from manifest import manifest_builder
from transfer import transfer_directory_pipelined

DOWNLOAD_ROOT = r"C:\Received_Movies"
MANIFEST_PATH = "manifest.json"
PORT = 5000
UDP_PORT = 5001


def discover_sender():
    udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    udp.bind(('', UDP_PORT))
    udp.settimeout(1.0)
    print("[*] Searching for Sender (Press Ctrl+C to enter IP manually)...")
    
    start = time.time()
    while time.time() - start < 15:
        try:
            udp.sendto(b"ANY_SENDER_OPEN?", ('255.255.255.255', UDP_PORT))
            data, addr = udp.recvfrom(1024)
            if data == b"SENDER_HERE":
                print(f"[+] Found Sender at {addr[0]}")
                return addr[0]
        except socket.timeout:
            continue
        except KeyboardInterrupt:
            break
    
    return input("[?] Enter Sender IP manually: ")


def run_receiver():
    sender_ip = discover_sender()
    if not sender_ip:
        return

    conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # 🔥 TCP performance tuning
    conn.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 32 * 1024 * 1024)
    conn.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 32 * 1024 * 1024)
    conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    conn.connect((sender_ip, PORT))
    
    networking.send_msg(conn, {"type": "METADATA_REQUEST"})
    msg, _ = networking.recv_msg(conn)
    metadata = msg["metadata"]

    if os.path.exists(MANIFEST_PATH):
        with open(MANIFEST_PATH, "r") as f:
            manifest = json.load(f)
    else:
        manifest = manifest_builder(metadata, MANIFEST_PATH, None, DOWNLOAD_ROOT)

    total_size = sum(f["size"] for f in manifest["files"].values())
    if total_size == 0:
        print("[!] Manifest is empty.")
        return

    os.makedirs(DOWNLOAD_ROOT, exist_ok=True)

    progress = {
        "transferred": 0,
        "total": total_size,
        "start_time": time.time()
    }

    transfer_directory_pipelined(
        conn,
        manifest,
        MANIFEST_PATH,
        progress,
        DOWNLOAD_ROOT
    )
    
    networking.send_msg(conn, {"type": "TRANSFER_COMPLETE"})
    conn.close()

    print("\n[DONE] Transfer finished successfully.")


if __name__ == "__main__":
    run_receiver()
