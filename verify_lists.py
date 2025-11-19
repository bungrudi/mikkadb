import socket
import sys
import time

def send_command(sock, command):
    sock.sendall(command.encode())
    response = sock.recv(4096).decode()
    return response

def parse_resp_integer(response):
    if not response: return None
    if response.startswith(':'):
        return int(response[1:].strip())
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

def main():
    host = '127.0.0.1'
    port = 6379
    
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))
        
        # Test 1: RPUSH new key
        print("Test 1: RPUSH new key")
        # Fixed: \r\n before mylist
        resp = send_command(sock, "*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$5\r\nitem1\r\n")
        val = parse_resp_integer(resp)
        if val == 1:
            print("PASS")
        else:
            print(f"FAIL: Expected 1, got {resp}")
            
        # Test 2: RPUSH multiple values
        print("Test 2: RPUSH multiple values")
        resp = send_command(sock, "*4\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$5\r\nitem2\r\n$5\r\nitem3\r\n")
        val = parse_resp_integer(resp)
        if val == 3:
            print("PASS")
        else:
            print(f"FAIL: Expected 3, got {resp}")
            
        # Test 3: LRANGE full
        print("Test 3: LRANGE full")
        resp = send_command(sock, "*4\r\n$6\r\nLRANGE\r\n$6\r\nmylist\r\n$1\r\n0\r\n$2\r\n-1\r\n")
        items = parse_resp_array(resp)
        if items == ['item1', 'item2', 'item3']:
            print("PASS")
        else:
            print(f"FAIL: Expected ['item1', 'item2', 'item3'], got {items}")
            
        # Test 4: LRANGE partial
        print("Test 4: LRANGE partial")
        resp = send_command(sock, "*4\r\n$6\r\nLRANGE\r\n$6\r\nmylist\r\n$1\r\n0\r\n$1\r\n1\r\n")
        items = parse_resp_array(resp)
        if items == ['item1', 'item2']:
            print("PASS")
        else:
            print(f"FAIL: Expected ['item1', 'item2'], got {items}")
            
        # Test 5: LRANGE negative indices
        print("Test 5: LRANGE negative indices")
        resp = send_command(sock, "*4\r\n$6\r\nLRANGE\r\n$6\r\nmylist\r\n$2\r\n-2\r\n$2\r\n-1\r\n")
        items = parse_resp_array(resp)
        if items == ['item2', 'item3']:
            print("PASS")
        else:
            print(f"FAIL: Expected ['item2', 'item3'], got {items}")

        # Test 6: LRANGE out of bounds
        print("Test 6: LRANGE out of bounds")
        resp = send_command(sock, "*4\r\n$6\r\nLRANGE\r\n$6\r\nmylist\r\n$2\r\n10\r\n$2\r\n20\r\n")
        items = parse_resp_array(resp)
        if items == []:
            print("PASS")
        else:
            print(f"FAIL: Expected [], got {items}")

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
    finally:
        sock.close()

if __name__ == "__main__":
    main()
