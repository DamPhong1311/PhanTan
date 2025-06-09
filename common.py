import hashlib
import socket
import json

NODES = ['127.0.0.1:5000', '127.0.0.1:5001', '127.0.0.1:5002']

def hash_key(key):
    return int(hashlib.sha256(key.encode()).hexdigest(), 16)

def get_node_for_key(key):
    """Primary node (1st responsible node)."""
    return NODES[hash_key(key) % len(NODES)]

def secondary_node_for_key(key):
    """Replica node (next node in ring)."""
    return NODES[(hash_key(key) + 1) % len(NODES)]

def get_nodes_for_key(key):
    """Return [primary, replica]"""
    return [get_node_for_key(key), secondary_node_for_key(key)]

def send_request(node_str, message):
    host, port = node_str.split(":")
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect((host, int(port)))
        s.sendall(json.dumps(message).encode())
        response = s.recv(4096).decode()
        return json.loads(response) if response else None
    except Exception as e:
        print(f"[Error] Cannot connect to {node_str}: {e}")
        return None
    finally:
        s.close()
