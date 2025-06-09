import socket
import json
import hashlib
from common import get_node_for_key, secondary_node_for_key, get_nodes_for_key
# Danh sách node đang chạy (thay đổi tùy hệ thống bạn)
NODES = [
    ("127.0.0.1", 5000),
    ("127.0.0.1", 5001),
    ("127.0.0.1", 5002)
]

# def hash_key(key):
#     return int(hashlib.sha256(key.encode()).hexdigest(), 16)

# def get_nodes_for_key(key):
#     """Trả về danh sách các node xử lý key này: primary và replica."""
#     key_hash = hash_key(key)
#     primary_idx = key_hash % len(NODES)
#     replica_idx = (primary_idx + 1) % len(NODES)
#     return [NODES[primary_idx], NODES[replica_idx]]

def send_message(host, port, message):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.settimeout(3)
        s.connect((host, port))
        s.sendall(json.dumps(message).encode())
        response = s.recv(4096).decode()
        return response
    except Exception as e:
        raise e
    finally:
        s.close()

def safe_send(key, message):
    for node in get_nodes_for_key(key):
        host, port = node
        try:
            response = send_message(host, port, message)
            print(f"Response from {host}:{port}:", response)
            return
        except Exception as e:
            print(f"[Warning] Failed to connect to {host}:{port} - {e}")
    print("All replicas failed. Could not complete request.")

if __name__ == "__main__":
    print("Client started.")
    while True:
        raw = input(">>> ").strip()
        if not raw:
            continue
        parts = raw.split()
        if parts[0] not in ["PUT", "GET", "DELETE"]:
            print("Invalid command.")
            continue

        cmd = parts[0]
        key = parts[1]
        value = parts[2] if cmd == "PUT" and len(parts) > 2 else None

        safe_send(key, {"cmd": cmd, "key": key, "value": value})
