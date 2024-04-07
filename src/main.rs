use std::io::{Read, Write};
// Uncomment this block to pass the first stage
use std::net::TcpListener;

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                println!("accepted new connection");
                let mut buffer: [u8;2048] = [0; 2048];
                'LOOP_COMMAND: while let Ok(bytes_read) = _stream.read(&mut buffer) {
                    println!("read {} bytes", bytes_read);
                    if bytes_read == 0 {
                        break 'LOOP_COMMAND;
                    }
                    let command = std::str::from_utf8(&buffer[..bytes_read]);
                    // TODO we'll deal with the command later
                    // now just assume its PING and return PONG
                    _stream.write_all(b"+PONG\r\n").unwrap();
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
