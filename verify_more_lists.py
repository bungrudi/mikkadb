import socket
import sys
import time
import threading

def send_command(sock, command):
    sock.sendall(command.encode())
    response = sock.recv(4096).decode()
    return response

def parse_resp_integer(response):
    if not response: return None
    if response.startswith(':'):
        return int(response[1:].strip())
    return None

def parse_resp_string(response):
    if not response: return None
    if response.startswith('$'):
        lines = response.split('\r\n')
        return lines[1]
    return None

def parse_resp_array(response):
    if not response: return None
    if response.startswith('*'):
        lines = response.split('\r\n')
        try:
            count = int(lines[0][1:])
        except ValueError:
            return None
            
        if count == -1:
            return None
        
        items = []
        idx = 1
        for _ in range(count):
            if idx >= len(lines): break
            if lines[idx].startswith('$'):
                try:
                    length = int(lines[idx][1:])
                except ValueError:
                    break
                idx += 1
                if idx >= len(lines): break
                items.append(lines[idx])
                idx += 1
        return items
    return None

def test_blpop_block():
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('127.0.0.1', 6379))
        print("BLPOP blocking...")
        # Block on 'mylist2' for 2 seconds
        resp = send_command(sock, "*3\r\n$5\r\nBLPOP\r\n$7\r\nmylist2\r\n$1\r\n2\r\n")
        print(f"BLPOP unblocked: {resp}")
        sock.close()
    except Exception as e:
        print(f"BLPOP thread error: {e}")

def main():
    host = '127.0.0.1'
    port = 6379
    
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))
        
        # Test 1: LPUSH
        print("Test 1: LPUSH")
        resp = send_command(sock, "*3\r\n$5\r\nLPUSH\r\n$7\r\nmylist2\r\n$5\r\nitem1\r\n")
        val = parse_resp_integer(resp)
        if val == 1:
            print("PASS")
        else:
            print(f"FAIL: Expected 1, got {resp}")
            
        # Test 2: LLEN
        print("Test 2: LLEN")
        resp = send_command(sock, "*2\r\n$4\r\nLLEN\r\n$7\r\nmylist2\r\n")
        val = parse_resp_integer(resp)
        if val == 1:
            print("PASS")
        else:
            print(f"FAIL: Expected 1, got {resp}")
            
        # Test 3: LPOP
        print("Test 3: LPOP")
        resp = send_command(sock, "*2\r\n$4\r\nLPOP\r\n$7\r\nmylist2\r\n")
        val = parse_resp_string(resp)
        if val == 'item1':
            print("PASS")
        else:
            print(f"FAIL: Expected item1, got {resp}")
            
        # Test 4: BLPOP immediate
        print("Test 4: BLPOP immediate")
        send_command(sock, "*3\r\n$5\r\nLPUSH\r\n$7\r\nmylist2\r\n$5\r\nitem2\r\n")
        resp = send_command(sock, "*3\r\n$5\r\nBLPOP\r\n$7\r\nmylist2\r\n$1\r\n0\r\n")
        items = parse_resp_array(resp)
        if items == ['mylist2', 'item2']:
            print("PASS")
        else:
            print(f"FAIL: Expected ['mylist2', 'item2'], got {items}")

        # Test 5: BLPOP block and unblock
        print("Test 5: BLPOP block and unblock")
        # Start a thread that blocks
        t = threading.Thread(target=test_blpop_block)
        t.start()
        time.sleep(0.5) # Wait for thread to block
        
        # Unblock it with LPUSH
        print("Unblocking with LPUSH...")
        send_command(sock, "*3\r\n$5\r\nLPUSH\r\n$7\r\nmylist2\r\n$5\r\nitem3\r\n")
        
        t.join()
        print("PASS (if thread printed unblocked message)")

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
    finally:
        sock.close()

if __name__ == "__main__":
    main()
