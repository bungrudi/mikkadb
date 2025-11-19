import socket
import time
import subprocess
import sys

def read_resp(sock):
    data = b""
    while True:
        chunk = sock.recv(1)
        if not chunk:
            raise ConnectionError("Socket closed")
        data += chunk
        if data.endswith(b"\r\n"):
            break
    
    line = data[:-2].decode()
    if not line:
        return None
        
    prefix = line[0]
    content = line[1:]
    
    if prefix == "+":
        return content
    elif prefix == "-":
        raise Exception(f"Redis Error: {content}")
    elif prefix == ":":
        return int(content)
    elif prefix == "$":
        length = int(content)
        if length == -1:
            return None
        data = b""
        while len(data) < length + 2:
            chunk = sock.recv(length + 2 - len(data))
            if not chunk:
                raise ConnectionError("Socket closed")
            data += chunk
        return data[:-2]
    elif prefix == "*":
        length = int(content)
        if length == -1:
            return None
        return [read_resp(sock) for _ in range(length)]
    else:
        raise Exception(f"Unknown prefix: {prefix} in line {line}")

def send_command(sock, args):
    cmd = f"*{len(args)}\r\n"
    for arg in args:
        cmd += f"${len(str(arg))}\r\n{arg}\r\n"
    sock.sendall(cmd.encode())

def run_test():
    server = subprocess.Popen(["./target/debug/mikkadb-rust", "--port", "6379"], 
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    time.sleep(1)
    
    try:
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect(("127.0.0.1", 6379))
        
        print("Test 1: XADD with auto-generated ID (*)")
        send_command(client, ["XADD", "mystream", "*", "foo", "bar"])
        id1 = read_resp(client).decode()
        print(f"Received ID: {id1}")
        
        print("Test 2: XADD with explicit ID")
        parts = id1.split("-")
        ms = int(parts[0])
        seq = int(parts[1])
        next_id = f"{ms+1}-0"
        send_command(client, ["XADD", "mystream", next_id, "foo", "bar2"])
        id2 = read_resp(client).decode()
        print(f"Received ID: {id2}")
        assert id2 == next_id
        
        print("Test 3: XADD with invalid ID (smaller)")
        try:
            send_command(client, ["XADD", "mystream", id1, "foo", "bar3"])
            read_resp(client)
            print("FAILED: Should have returned error")
        except Exception as e:
            print(f"PASSED: Received expected error: {e}")

        print("Test 4: XADD with 0-0")
        try:
            send_command(client, ["XADD", "mystream", "0-0", "foo", "bar4"])
            read_resp(client)
            print("FAILED: Should have returned error for 0-0")
        except Exception as e:
            print(f"PASSED: Received expected error: {e}")
            
        print("Test 5: XADD with partial auto-generation")
        next_ms = ms + 2
        send_command(client, ["XADD", "mystream", f"{next_ms}-*", "foo", "bar5"])
        id3 = read_resp(client).decode()
        print(f"Received ID: {id3}")
        assert id3 == f"{next_ms}-0"
        
        print("Test 6: XREAD single stream")
        send_command(client, ["XREAD", "STREAMS", "mystream", "0-0"])
        resp = read_resp(client)
        # Expect: [[b'mystream', [[b'1763527279738-0', [b'foo', b'bar']], ...]]]
        print(f"XREAD response: {resp}")
        assert len(resp) == 1
        assert resp[0][0] == b"mystream"
        entries = resp[0][1]
        assert len(entries) >= 3
        
        print("Test 7: XREAD multiple streams")
        send_command(client, ["XADD", "otherstream", "*", "key", "val"])
        other_id = read_resp(client).decode()
        
        send_command(client, ["XREAD", "STREAMS", "mystream", "otherstream", "0-0", "0-0"])
        resp = read_resp(client)
        assert len(resp) == 2
        assert resp[0][0] == b"mystream"
        assert resp[1][0] == b"otherstream"
        
        print("Test 8: XREAD with $")
        send_command(client, ["XREAD", "STREAMS", "mystream", "$"])
        resp = read_resp(client)
        assert resp is None # Should be null if no new entries
        
        # Add new entry and read with $ (simulated, since we can't block yet)
        # Actually, $ means "ids greater than max id". If we add one, then read with $, we won't get it unless we read AFTER adding.
        # But $ resolves to current max ID. So if we XREAD ... $, we get nothing.
        # Then if we XADD, and XREAD ... last_id, we get it.
        # Test $ resolution:
        # 1. Get last ID
        # 2. XREAD ... $ -> Null
        # 3. XADD
        # 4. XREAD ... $ -> Null (because $ is NOW the new max)
        # So $ is mostly useful for BLOCK.
        # Let's just verify it returns Null for now.
        
        print("Test 9: XREAD BLOCK timeout")
        start = time.time()
        send_command(client, ["XREAD", "BLOCK", "1000", "STREAMS", "mystream", "$"])
        resp = read_resp(client)
        duration = time.time() - start
        assert resp is None
        assert duration >= 1.0
        print(f"Blocked for {duration:.2f}s")
        
        print("Test 10: XREAD BLOCK with data")
        # We need a separate thread or process to add data while we block
        import threading
        
        def add_data():
            time.sleep(0.5)
            c2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            c2.connect(("127.0.0.1", 6379))
            send_command(c2, ["XADD", "mystream", "*", "foo", "blocked_val"])
            c2.close()
            
        t = threading.Thread(target=add_data)
        t.start()
        
        start = time.time()
        send_command(client, ["XREAD", "BLOCK", "2000", "STREAMS", "mystream", "$"])
        resp = read_resp(client)
        duration = time.time() - start
        t.join()
        
        print(f"Blocked for {duration:.2f}s")
        assert resp is not None
        assert len(resp) == 1
        assert resp[0][0] == b"mystream"
        entries = resp[0][1]
        assert len(entries) == 1
        assert entries[0][1][1] == b"blocked_val"
        
        print("All tests passed!")
            
    finally:
        server.terminate()
        server.wait()

if __name__ == "__main__":
    run_test()
