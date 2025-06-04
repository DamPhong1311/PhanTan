import socket
import threading
import time
import json
import os
import hashlib

# ==== Config ====
HOST = '127.0.0.1'
PORT = int(input("Enter port for this node: "))
NODES = [
    '127.0.0.1:5000',
    '127.0.0.1:5001',
    '127.0.0.1:5002'
]
DATA_FILE = f"data_{PORT}.json"

# ==== Data ====
DATA_PRIMARY = {}
DATA_REPLICA = {}
DATA_LOCK = threading.Lock()
DATA_CHANGED = False

ALIVE_NODES = NODES.copy()

# ==== Functions ====

def get_node_for_key(key):
    key_hash = int(hashlib.sha256(key.encode()).hexdigest(), 16)
    return NODES[key_hash % len(NODES)]

def secondary_node_for_key(key):
    key_hash = int(hashlib.sha256(key.encode()).hexdigest(), 16)
    return NODES[(key_hash + 1) % len(NODES)]

def load_data():
    global DATA_PRIMARY, DATA_REPLICA
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "r") as f:
                saved = json.load(f)
                DATA_PRIMARY = saved.get("primary", {})
                DATA_REPLICA = saved.get("replica", {})
            print(f"[Load] Data loaded from {DATA_FILE}")
        except (json.JSONDecodeError, ValueError):
            print(f"[Load] Warning: {DATA_FILE} is empty or corrupted. Starting with empty data.")
            DATA_PRIMARY = {}
            DATA_REPLICA = {}
    else:
        DATA_PRIMARY = {}
        DATA_REPLICA = {}

def save_data_periodically():
    global DATA_CHANGED
    while True:
        time.sleep(5)
        if DATA_CHANGED:
            with DATA_LOCK:
                with open(DATA_FILE, "w") as f:
                    json.dump({
                        "primary": DATA_PRIMARY,
                        "replica": DATA_REPLICA
                    }, f)
                DATA_CHANGED = False
            print(f"[Save] Data saved to {DATA_FILE}")

def send_request(addr, request):
    host, port = addr.split(":")
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(3)
        s.connect((host, int(port)))
        s.sendall(json.dumps(request).encode())
        response = s.recv(65536).decode()
        return json.loads(response)

def handle_client(conn, addr):
    try:
        request = json.loads(conn.recv(65536).decode())
        cmd = request.get("cmd")
        key = request.get("key")
        value = request.get("value")

        if cmd == "SNAPSHOT":
            with DATA_LOCK:
                merged = {**DATA_PRIMARY, **DATA_REPLICA}
                conn.sendall(json.dumps(merged).encode())
            return

        if key:
            responsible_node = get_node_for_key(key)
            replica_node = secondary_node_for_key(key)
            current_node = f"{HOST}:{PORT}"

            if cmd in ["PUT", "GET", "DELETE"] and responsible_node != current_node:
                result = send_request(responsible_node, request)
                conn.sendall(json.dumps(result).encode())
                return

            with DATA_LOCK:
                global DATA_CHANGED
                if cmd == "PUT":
                    if value is None:
                        conn.sendall(json.dumps({"status": "MISSING VALUE"}).encode())
                        return
                    DATA_PRIMARY[key] = value
                    DATA_CHANGED = True

                    if replica_node != current_node:
                        try:
                            send_request(replica_node, {
                                "cmd": "PUT_REPLICA",
                                "key": key,
                                "value": value
                            })
                        except Exception as e:
                            print(f"[Replica Error] Could not send to replica {replica_node}: {e}")

                    conn.sendall(json.dumps({"status": "OK"}).encode())

                elif cmd == "GET":
                    val = DATA_PRIMARY.get(key) or DATA_REPLICA.get(key)
                    conn.sendall(json.dumps({key: val}).encode())

                elif cmd == "DELETE":
                    DATA_PRIMARY.pop(key, None)
                    DATA_REPLICA.pop(key, None)
                    DATA_CHANGED = True

                    # Gửi yêu cầu xóa tới replica
                    if replica_node != current_node:
                        try:
                            send_request(replica_node, {
                                "cmd": "DELETE_REPLICA",
                                "key": key
                            })
                        except Exception as e:
                            print(f"[Replica Error] Could not delete from replica {replica_node}: {e}")

                    conn.sendall(json.dumps({"status": "DELETED"}).encode())

                elif cmd == "PUT_REPLICA":
                    DATA_REPLICA[key] = value
                    DATA_CHANGED = True
                    conn.sendall(json.dumps({"status": "REPLICA_OK"}).encode())

                elif cmd == "DELETE_REPLICA":
                    DATA_REPLICA.pop(key, None)
                    DATA_CHANGED = True
                    conn.sendall(json.dumps({"status": "REPLICA_DELETED"}).encode())

                else:
                    conn.sendall(json.dumps({"status": "INVALID CMD"}).encode())

    except Exception as e:
        print(f"[Error] {e}")
        try:
            conn.sendall(json.dumps({"status": "ERROR", "msg": str(e)}).encode())
        except:
            pass
    finally:
        conn.close()

def start_server():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((HOST, PORT))
    server.listen()
    print(f"[Start] Server running on {HOST}:{PORT}")
    while True:
        conn, addr = server.accept()
        threading.Thread(target=handle_client, args=(conn, addr)).start()

def request_snapshot():
    current_node = f"{HOST}:{PORT}"
    for node in ALIVE_NODES:
        if node == current_node:
            continue
        try:
            response = send_request(node, {"cmd": "SNAPSHOT"})
            if response:
                recovered = {}
                for key, value in response.items():
                    if get_node_for_key(key) == current_node:
                        recovered[key] = value
                with DATA_LOCK:
                    global DATA_CHANGED, DATA_PRIMARY
                    DATA_PRIMARY = recovered
                    DATA_CHANGED = True
                    with open(DATA_FILE, "w") as f:
                        json.dump({"primary": DATA_PRIMARY, "replica": {}}, f)
                print(f"[Recovery] Data recovered from {node}")
                return
        except Exception as e:
            print(f"[Recovery] Failed from {node}: {e}")

# ==== Main ====
load_data()

# In ra node chính để kiểm tra hash ổn định
print(f"[Debug] Primary node for 'name2': {get_node_for_key('name2')}")

if not DATA_PRIMARY:
    request_snapshot()

threading.Thread(target=save_data_periodically, daemon=True).start()
start_server()
