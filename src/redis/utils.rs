use std::net::TcpStream;
use std::io::{Read, Write};
use std::thread;
use std::time::Duration;
use std::borrow::Cow;

// Helper function to generate a replica ID
pub fn gen_replid() -> &'static str {
    // todo: generate a random replid
    "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
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
pub fn read_response<'a>(stream: &mut TcpStream, buffer: &'a mut [u8; 512]) -> std::io::Result<Cow<'a, str>> {
    let bytes_read = stream.read(buffer)?;
    Ok(String::from_utf8_lossy(&buffer[0..bytes_read])) // Zero-copy conversion using Cow
}

#[inline]
pub fn read_until_end_of_rdb(stream: &mut TcpStream, buffer: &mut [u8; 512]) {
    let mut count = 0;
    'LOOP_PEEK: while let Ok(peek_size) = stream.peek(buffer) {
        thread::sleep(Duration::from_millis(200));
        println!("peek_size: {}", peek_size);
        
        // Zero-copy string conversion for debug output
        println!("peeked: {}", String::from_utf8_lossy(&buffer[0..peek_size]));
        
        if count > 10 {
            println!("count exceeded");
            break;
        }
        if peek_size == 0 {
            count += 1;
            continue;
        }

        // find the pattern '$xx\r\n' where xx is numeric in the buffer
        let mut found = false;
        for i in 0..peek_size {
            if buffer[i] == b'$' {
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
                    
                    // Pre-allocate buffer with exact size needed
                    let mut rdb_buffer = vec![0; i + length + 5];
                    if let Ok(_) = stream.read_exact(&mut rdb_buffer) {
                        // Zero-copy string conversion for debug output
                        println!("rdb_file: {}", String::from_utf8_lossy(&rdb_buffer));
                    }
                    break 'LOOP_PEEK;
                }
            }
        }
    }
}
