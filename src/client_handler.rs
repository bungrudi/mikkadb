use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::{Arc, Mutex};
use std::thread;
use crate::resp::parse_resp;

// ClientHandler should ideally be an actor.
pub struct ClientHandler {
    client: Arc<Mutex<TcpStream>>,
}

impl ClientHandler {
    const PING : &'static [&'static str] = &["PING", "ping"];
    const ECHO : &'static [&'static str] = &["ECHO", "echo"];

    pub fn new (client: TcpStream) -> Self {
        ClientHandler {
            client: Arc::new(Mutex::new(client)),
        }
    }

    pub fn start(&mut self) {
        let mut buffer: [u8;2048] = [0; 2048];
        let client = Arc::clone(&self.client);
        thread::spawn(move || {
            let mut client = client.lock().unwrap();
            'LOOP_COMMAND: while let Ok(bytes_read) = client.read(&mut buffer) {
                println!("read {} bytes", bytes_read);
                if bytes_read == 0 {
                    break 'LOOP_COMMAND;
                }
                let commands = parse_resp(&buffer, bytes_read);
                let command = commands[0];
                if Self::PING.contains(&command.command) {
                    client.write_all(b"+PONG\r\n").unwrap();
                } else if Self::ECHO.contains(&command.command) {
                    client.write_all(b"+").unwrap();
                    client.write_all(command.data.as_bytes()).unwrap();
                    client.write_all(b"\r\n").unwrap();
                } else {
                    client.write_all(b"-ERR unknown command\r\n").unwrap();
                }
            }
            println!("closing connection {}", client.peer_addr().unwrap());
        });
    }
}