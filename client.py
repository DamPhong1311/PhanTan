import socket
import json

def send(host, port, message):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.settimeout(5)  # Thiết lập timeout để tránh treo
        s.connect((host, port))
        s.sendall(json.dumps(message).encode())
        result = s.recv(4096).decode()
        print("Response:", result)
    except Exception as e:
        print(f"Error: {e}")
    finally:
        s.close()  # Luôn đóng socket

if __name__ == "__main__":
    port = int(input("Nhập cổng muốn kết nối: "))
    while True:
        raw = input(">>> ").strip()
        if not raw:
            continue
        parts = raw.split()
        if parts[0] not in ["PUT", "GET", "DELETE"]:
            continue

        cmd = parts[0]
        key = parts[1]
        value = parts[2] if cmd == "PUT" and len(parts) > 2 else None

        send("127.0.0.1", port, {"cmd": cmd, "key": key, "value": value})
