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

    # Nếu không có dữ liệu, thử khôi phục từ snapshot
    if not DATA_PRIMARY:
        print(f"[Recovery] No primary data found. Trying to recover from other nodes...")
        request_snapshot()
    
    sync_replicas_on_startup()

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

def check_alive_nodes():
    global ALIVE_NODES
    while True:
        new_alive = []
        for node in NODES:
            if node == f"{HOST}:{PORT}":
                new_alive.append(node)
                continue
            try:
                resp = send_request(node, {"cmd": "PING"})
                if resp.get("status") == "ALIVE":
                    new_alive.append(node)
            except:
                print(f"[HealthCheck] Node {node} unreachable.")
        ALIVE_NODES = new_alive
        print(f"[HealthCheck] Alive nodes: {ALIVE_NODES}")
        time.sleep(10)

def sync_replicas_on_startup():
    current_node = f"{HOST}:{PORT}"
    print(f"[Sync] Starting replica synchronization for {current_node}")

    # Lấy dữ liệu replica từ các node khác
    for node in ALIVE_NODES:
        if node == current_node:
            continue
        try:
            # Lấy snapshot từ node khác
            snapshot = send_request(node, {"cmd": "SNAPSHOT"})
            # snapshot là dict key -> value
            for key, value in snapshot.items():
                correct_primary = get_node_for_key(key)
                correct_replica = secondary_node_for_key(key)
                with DATA_LOCK:
                    # Nếu mình là primary của key đó, nhưng chưa có hoặc giá trị khác trong primary
                    if correct_primary == current_node:
                        if DATA_PRIMARY.get(key) != value:
                            DATA_PRIMARY[key] = value
                            print(f"[Sync] Updated primary key '{key}' from node {node}")
                            global DATA_CHANGED
                            DATA_CHANGED = True
                    # Nếu mình là replica của key đó, cũng cập nhật replica
                    elif correct_replica == current_node:
                        if DATA_REPLICA.get(key) != value:
                            DATA_REPLICA[key] = value
                            print(f"[Sync] Updated replica key '{key}' from node {node}")
                            DATA_CHANGED = True
        except Exception as e:
            print(f"[Sync] Failed to get snapshot from {node}: {e}")

    # Sau khi cập nhật, lưu dữ liệu xuống file
    with DATA_LOCK:
        with open(DATA_FILE, "w") as f:
            json.dump({
                "primary": DATA_PRIMARY,
                "replica": DATA_REPLICA
            }, f)
    print(f"[Sync] Replica synchronization completed.")

def handle_client(conn, addr):
    try:
        request = json.loads(conn.recv(65536).decode())
        cmd = request.get("cmd")
        key = request.get("key")
        value = request.get("value")

        if cmd == "PING":
            conn.sendall(json.dumps({"status": "ALIVE"}).encode())
            return

        if cmd == "SNAPSHOT":
            with DATA_LOCK:
                merged = {**DATA_PRIMARY, **DATA_REPLICA}
                conn.sendall(json.dumps(merged).encode())
            return

        if key:
            responsible_node = get_node_for_key(key)
            replica_node = secondary_node_for_key(key)
            current_node = f"{HOST}:{PORT}"

            global DATA_CHANGED

            if cmd in ["PUT", "GET", "DELETE"]:
                if responsible_node != current_node and responsible_node not in ALIVE_NODES:
                    if replica_node == current_node:
                        print(f"[Fallback] Primary {responsible_node} died. Acting as fallback for key '{key}'")
                        with DATA_LOCK:
                            if cmd == "GET":
                                val = DATA_REPLICA.get(key)
                                conn.sendall(json.dumps({key: val}).encode())
                            elif cmd == "DELETE":
                                DATA_REPLICA.pop(key, None)
                                DATA_CHANGED = True
                                conn.sendall(json.dumps({"status": "REPLICA_DELETED"}).encode())
                            elif cmd == "PUT":
                                if value is None:
                                    conn.sendall(json.dumps({"status": "MISSING VALUE"}).encode())
                                    return
                                DATA_REPLICA[key] = value
                                DATA_CHANGED = True
                                conn.sendall(json.dumps({"status": "REPLICA_PUT"}).encode())
                        return
                    else:
                        conn.sendall(json.dumps({"status": "ERROR", "msg": f"Primary node {responsible_node} unreachable"}).encode())
                        return

                if responsible_node != current_node:
                    try:
                        result = send_request(responsible_node, request)
                        conn.sendall(json.dumps(result).encode())
                    except Exception as e:
                        conn.sendall(json.dumps({"status": "ERROR", "msg": f"Failed to reach primary node: {e}"}).encode())
                    return

            with DATA_LOCK:
                if cmd == "PUT":
                    if value is None:
                        conn.sendall(json.dumps({"status": "MISSING VALUE"}).encode())
                        return
                    DATA_PRIMARY[key] = value
                    DATA_CHANGED = True

                    if replica_node != current_node and replica_node in ALIVE_NODES:
                        try:
                            send_request(replica_node, {
                                "cmd": "PUT_REPLICA",
                                "key": key,
                                "value": value
                            })
                        except Exception as e:
                            print(f"[Replica Error] Could not send to replica {replica_node}: {e}")
                    else:
                        print(f"[Replica Warning] Replica node {replica_node} is down. Skipping replication.")

                    conn.sendall(json.dumps({"status": "OK"}).encode())

                elif cmd == "GET":
                    val = DATA_PRIMARY.get(key) or DATA_REPLICA.get(key)
                    conn.sendall(json.dumps({key: val}).encode())

                elif cmd == "DELETE":
                    DATA_PRIMARY.pop(key, None)
                    DATA_REPLICA.pop(key, None)
                    DATA_CHANGED = True

                    if replica_node != current_node and replica_node in ALIVE_NODES:
                        try:
                            send_request(replica_node, {
                                "cmd": "DELETE_REPLICA",
                                "key": key
                            })
                        except Exception as e:
                            print(f"[Replica Error] Could not delete from replica {replica_node}: {e}")
                    else:
                        print(f"[Replica Warning] Replica node {replica_node} is down. Skipping replica delete.")

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
    recovered_primary = {}
    recovered_replica = {}

    print(f"[Recovery] Start snapshot recovery for {current_node}")
    
    for node in ALIVE_NODES:
        if node == current_node:
            continue
        try:
            response = send_request(node, {"cmd": "SNAPSHOT"})
            if response:
                for key, value in response.items():
                    correct_primary = get_node_for_key(key)
                    correct_replica = secondary_node_for_key(key)

                    # Nếu key này đúng ra phải là primary ở node hiện tại
                    if correct_primary == current_node:
                        recovered_primary[key] = value
                    # Nếu key này đúng ra phải là replica ở node hiện tại
                    elif correct_replica == current_node:
                        recovered_replica[key] = value
        except Exception as e:
            print(f"[Recovery] Failed from {node}: {e}")

    with DATA_LOCK:
        global DATA_CHANGED, DATA_PRIMARY, DATA_REPLICA
        DATA_PRIMARY = recovered_primary
        DATA_REPLICA = recovered_replica
        DATA_CHANGED = True

        with open(DATA_FILE, "w") as f:
            json.dump({
                "primary": DATA_PRIMARY,
                "replica": DATA_REPLICA
            }, f)

    print(f"[Recovery] Recovered {len(DATA_PRIMARY)} primary and {len(DATA_REPLICA)} replica keys")

    # Gửi lại các replica tương ứng đến các node khác
    for key, value in recovered_primary.items():
        replica_node = secondary_node_for_key(key)
        if replica_node != current_node and replica_node in ALIVE_NODES:
            try:
                send_request(replica_node, {
                    "cmd": "PUT_REPLICA",
                    "key": key,
                    "value": value
                })
                print(f"[Sync] Sent replica for key '{key}' to {replica_node}")
            except Exception as e:
                print(f"[Sync Error] Failed to sync replica to {replica_node}: {e}")


# ==== Main ====
load_data()

threading.Thread(target=check_alive_nodes, daemon=True).start()
threading.Thread(target=save_data_periodically, daemon=True).start()

start_server()
