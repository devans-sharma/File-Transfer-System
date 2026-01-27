import os, json, hashlib, time, networking
from collections import deque

PIPELINE_DEPTH = 8 

def transfer_directory_pipelined(conn, manifest, manifest_path, progress, dest_root):
    chunk_size = manifest["chunk_size"]
    uncommitted_count = 0

    for rel_path, file_entry in manifest["files"].items():
        # Sanitize path for Windows
        clean_rel = rel_path.replace("/", os.sep).lstrip(os.sep)
        dest_path = os.path.join(dest_root, clean_rel)
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)

        if not os.path.exists(dest_path):
            with open(dest_path, "wb") as f:
                f.truncate(file_entry["size"])

        pending = deque()
        with open(dest_path, "r+b") as dest:
            for chunk_index, chunk in file_entry["chunks"].items():
                if chunk["status"] == "VERIFIED": continue

                networking.send_msg(conn, {
                    "type": "CHUNK_REQUEST",
                    "path": rel_path,
                    "chunk_index": int(chunk_index)
                })
                pending.append((chunk_index, chunk))

                if len(pending) >= PIPELINE_DEPTH:
                    uncommitted_count += 1
                    save_now = (uncommitted_count >= 10) 
                    _receive_and_write(conn, pending, dest, rel_path, chunk_size, manifest, manifest_path, progress, save_now)
                    if save_now: uncommitted_count = 0

            while pending:
                _receive_and_write(conn, pending, dest, rel_path, chunk_size, manifest, manifest_path, progress, True)
        
        file_entry["completed"] = True
        with open(manifest_path, "w") as f: json.dump(manifest, f, indent=4)

def _receive_and_write(conn, pending, dest, rel_path, chunk_size, manifest, manifest_path, progress, save_manifest):
    chunk_index, chunk = pending.popleft()
    _, data = networking.recv_msg(conn)
    if not data: raise RuntimeError("Sender disconnected")

    if hashlib.sha256(data).hexdigest() != chunk["expected_hash"]:
        raise RuntimeError(f"Hash mismatch at {rel_path}:{chunk_index}")

    dest.seek(int(chunk_index) * chunk_size)
    dest.write(data)

    progress["transferred"] += len(data)
    elapsed = max(time.time() - progress["start_time"], 0.001)
    speed = (progress["transferred"] / (1024 * 1024)) / elapsed
    print(f"\rProgress: {(progress['transferred']/progress['total'])*100:.2f}% | Speed: {speed:.2f} MB/s", end="")

    chunk["status"] = "VERIFIED"
    if save_manifest:
        with open(manifest_path, "w") as f: json.dump(manifest, f, indent=4)