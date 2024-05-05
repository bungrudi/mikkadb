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
        const NEWLINE: &str = "\r\n";
        let mut buffer: [u8;2048] = [0; 2048];
        let client = Arc::clone(&self.client);
        let redis = Arc::clone(&self.redis);
        thread::spawn(move || {
            let mut client = client.lock().unwrap();
            'LOOP_COMMAND: while let Ok(bytes_read) = client.read(&mut buffer) {
                println!("read {} bytes", bytes_read);
                if bytes_read == 0 {
                    break 'LOOP_COMMAND;
                }
                // TODO refactor this to use a buffer pool.
                let commands = parse_resp(&buffer, bytes_read);

                let command = &commands[0];
                // TODO use the same buffer to write the response.
                // actually is there benefit in re-using the buffer?
                match redis.lock().unwrap().execute_command(command, Some(&mut client)) {
                    // TODO check if write! is actually the best performance wise.
                    Ok(response) => {
                        if(!response.is_empty())  {
                            client.write(response.as_bytes());
                        }
                    }, //write!(client, "{}", response).unwrap(),
                    Err(error) => {client.write(error.as_bytes());}//write!(client, "{}", error).unwrap(),
                };
            }
            println!("closing connection {}", client.peer_addr().unwrap());
        });
    }
}