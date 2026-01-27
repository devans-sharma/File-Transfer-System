import json
import os 


def manifest_builder(source_metadata, manifest_path, source_root, dest_root):
    manifest = {
        "version": source_metadata["version"],
        "chunk_size": source_metadata["chunk_size"],
        "hash_algo": source_metadata.get("hash_algo", "sha256"),
        "source_root": source_root,
        "destination_root": dest_root,
        "files": {}
    }

    for rel_path, file_info in source_metadata["files"].items():
        chunks = {}

        for chunk_index, chunk_info in file_info["chunks"].items():
            chunks[chunk_index] = {
                "status": "MISSING",
                "expected_hash": chunk_info["hash"]
            }

        manifest["files"][rel_path] = {
            "size": file_info["size"],
            "total_chunks": file_info["total_chunks"],
            "completed": False,
            "chunks": chunks
        }

    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=4)

    return manifest


# with open("data.json", "r") as f:
    # source_metadata = json.load(f)

# real = manifest_builder(source_metadata, r"D:\manifest_path\manifest.json")

# print(real)