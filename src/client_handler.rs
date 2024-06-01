use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::{Arc, Mutex};
use std::thread;
use crate::redis::{Redis};
use crate::resp::parse_resp;

// ClientHandler should ideally be an actor.
pub struct ClientHandler {
    client: Arc<Mutex<TcpStream>>,
    redis: Arc<Mutex<Redis>>
}

impl ClientHandler {

    pub fn new (client: TcpStream, redis: Arc<Mutex<Redis>>) -> Self {
        ClientHandler {
            client: Arc::new(Mutex::new(client)),
            redis
        }
    }

    pub fn start(&mut self) {
        let mut buffer: [u8;2048] = [0; 2048];
        let client = Arc::clone(&self.client);
        let redis = Arc::clone(&self.redis);
        thread::spawn(move || {
            let mut client = client.lock().unwrap();
            let addr = client.peer_addr().unwrap();
            'LOOP_COMMAND: while let Ok(bytes_read) = client.read(&mut buffer) {
                println!("read {} bytes", bytes_read);
                if bytes_read == 0 {
                    break 'LOOP_COMMAND;
                }
                // TODO refactor this to use a buffer pool.
                let commands = parse_resp(&buffer, bytes_read);

                let command = &commands[0];
                println!("command: {:?}", command);
                // TODO use the same buffer to write the response.
                // actually is there benefit in re-using the buffer?
                match redis.lock().expect("failed to lock redis").execute_command(command, Some(&mut client)) {
                    // TODO check if write! is actually the best performance wise.
                    Ok(response) => {
                        println!("response: {}", response);
                        if !response.is_empty() {
                            let _ = client.write(response.as_bytes());
                        }
                    }, //write!(client, "{}", response).unwrap(),
                    Err(error) => {
                        eprintln!("error: {}", error);
                        let _ = client.write(error.as_bytes());
                    }
                };
                println!("after write");
            }
            println!("closing connection {}", addr);
        });
    }
}