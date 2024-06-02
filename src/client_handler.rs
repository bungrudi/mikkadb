use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::{Arc, Mutex};
use std::thread;
use crate::redis::{Redis, RedisCommand};
use crate::resp::parse_resp;

// ClientHandler should ideally be an actor.
pub struct ClientHandler {
    client: Arc<Mutex<TcpStream>>,
    redis: Arc<Mutex<Redis>>,
    master: bool // is this client a master?
}

impl ClientHandler {

    pub fn new (client: TcpStream, redis: Arc<Mutex<Redis>>) -> Self {
        ClientHandler {
            client: Arc::new(Mutex::new(client)),
            redis,
            master: false
        }
    }

    pub fn new_master (client: TcpStream, redis: Arc<Mutex<Redis>>) -> Self {
        ClientHandler {
            client: Arc::new(Mutex::new(client)),
            redis,
            master: true
        }
    }

    pub fn start(&mut self) {
        let mut buffer: [u8;2048] = [0; 2048];
        let client = Arc::clone(&self.client);
        let redis = Arc::clone(&self.redis);
        let master = self.master;
        thread::spawn(move || {
            let mut client = client.lock().unwrap();
            let addr = client.peer_addr().unwrap();
            'LOOP_READ_NETWORK: while let Ok(bytes_read) = client.read(&mut buffer) {
                // TODO disconnection hook.. this is handy when we want to do cleanup i.e. in a replica setup
                println!("read {} bytes", bytes_read);
                if bytes_read == 0 {
                    break 'LOOP_READ_NETWORK;
                }
                // TODO refactor this to use a buffer pool.
                let commands = parse_resp(&buffer, bytes_read);

                // let command = &commands[0];
                // iterate over commands
                'LOOP_REDIS_CMD: for command in commands {
                    if matches!(command, RedisCommand::None) {
                        break 'LOOP_REDIS_CMD;
                    }
                    println!("command: {:?}", command);
                    // TODO use the same buffer to write the response.
                    // actually is there benefit in re-using the buffer?
                    match redis.lock().expect("failed to lock redis").execute_command(&command, Some(&mut client)) {
                        // TODO check if write! is actually the best performance wise.
                        Ok(response) => {
                            println!("response: {}", response);
                            if !response.is_empty() && !master {
                                let _ = client.write(response.as_bytes());
                            }
                            if master {
                                println!("master, not writing to client");
                            }
                        }, //write!(client, "{}", response).unwrap(),
                        Err(error) => {
                            eprintln!("error: {}", error);
                            if !master {
                                let _ = client.write(error.as_bytes());
                            }
                        }
                    };
                    println!("after write");
                }

            }
            println!("closing connection {}", addr);
        });
    }
}