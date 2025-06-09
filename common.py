import socket
import json

NODES = ['127.0.0.1:5000', '127.0.0.1:5001', '127.0.0.1:5002']

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
