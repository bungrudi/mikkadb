use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::{Arc, Mutex};
use std::thread;

// ClientHandler should ideally be an actor.
pub struct ClientHandler {
    client: Arc<Mutex<TcpStream>>,
}

impl ClientHandler {
    pub fn new (client: TcpStream) -> Self {
        ClientHandler {
            client: Arc::new(Mutex::new(client)),
        }
    }

    pub fn start(&mut self) {
        let mut buffer: [u8;2048] = [0; 2048];
        let mut client = Arc::clone(&self.client);
        thread::spawn(move || {
            let mut client = client.lock().unwrap();
            'LOOP_COMMAND: while let Ok(bytes_read) = client.read(&mut buffer) {
                println!("read {} bytes", bytes_read);
                if bytes_read == 0 {
                    break 'LOOP_COMMAND;
                }
                let command = std::str::from_utf8(&buffer[..bytes_read]);
                // TODO we'll deal with the command later
                // now just assume its PING and return PONG
                client.write_all(b"+PONG\r\n").unwrap();
            }
            println!("closing connection {}", client.peer_addr().unwrap());
        });
    }
}