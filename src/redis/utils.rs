use std::net::TcpStream;
use std::io::{Read, Write};
use std::thread;
use std::time::Duration;

// Helper function to generate a replica ID
pub fn gen_replid() -> String {
    // todo: generate a random replid
    "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string()
}

#[inline]
pub fn connect_to_server(host: &str, port: &str) -> std::io::Result<TcpStream> {
    let replicaof_addr = format!("{}:{}", host, port);
    TcpStream::connect(replicaof_addr)
}

#[inline]
pub fn send_command(stream: &mut std::net::TcpStream, command: &str) -> std::io::Result<usize> {
    stream.write(command.as_bytes())
}

#[inline]
pub fn read_response(stream: &mut TcpStream, buffer: &mut [u8; 512]) -> std::io::Result<String> {
    let bytes_read = stream.read(buffer)?;
    Ok(String::from_utf8_lossy(&buffer[0..bytes_read]).to_string()) // TODO ready the RDB part
}

#[inline]
pub fn read_until_end_of_rdb(stream: &mut TcpStream, buffer: &mut [u8; 512]) {
    // peek until we get '$' and then read the length of the 'fullresync'+RDB file.
    let mut count = 0;
    'LOOP_PEEK: while let Ok(peek_size) = stream.peek(buffer) {
        thread::sleep(Duration::from_millis(200));
        println!("peek_size: {}", peek_size);
        println!("peeked: {}", String::from_utf8_lossy(&buffer[0..peek_size]));
        if count > 10 {
            println!("count exceeded");
            break;
        }
        if peek_size == 0 {
            count += 1;
            continue;
        }

        // find the patter '$xx\r\n' where xx is numeric in the buffer, then read from 0 to current pos+88
        let mut found = false;
        let mut ret_len;
        for i in 0..peek_size {
            if buffer[i] == b'$' {
                ret_len = i;
                let mut length = 0;
                let mut j = i + 1;
                while j < peek_size {
                    if buffer[j] == b'\r' {
                        found = true;
                        break;
                    }
                    length = length * 10 + (buffer[j] - b'0') as usize;
                    j += 1;
                }
                if found {
                    println!("found RDB length: {}", length);
                    let mut rdb_buffer = vec![0; ret_len + length + 5]; // 2 would be \r\n
                    let _ = stream.read_exact(&mut rdb_buffer);
                    let rdb_file = String::from_utf8_lossy(&rdb_buffer);
                    println!("rdb_file: {}", rdb_file);
                    break 'LOOP_PEEK;
                }
            }
        }
    }
}
