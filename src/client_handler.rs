use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::{Arc, Mutex};
use std::thread;
use crate::redis::{Redis, RedisCommand};
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
            'LOOP_COMMAND: while let Ok(bytes_read) = client.read(&mut buffer) {
                println!("read {} bytes", bytes_read);
                if bytes_read == 0 {
                    break 'LOOP_COMMAND;
                }
                let commands = parse_resp(&buffer, bytes_read);

                let command = commands.get(0).unwrap();
                match command {
                    RedisCommand::Ping => {
                        client.write_all(b"+PONG\r\n").unwrap();
                    },
                    RedisCommand::Echo { data } => {
                        client.write_all(b"+").unwrap();
                        client.write_all(data.as_bytes()).unwrap();
                        client.write_all(b"\r\n").unwrap();
                    },
                    RedisCommand::Get { key } => {
                        let value = redis.lock().unwrap().get(key);
                        match value {
                            Some(value) => {
                                client.write_all(b"+").unwrap();
                                client.write_all(value.as_bytes()).unwrap();
                                client.write_all(b"\r\n").unwrap();
                            },
                            None => {
                                client.write_all(b"$-1\r\n").unwrap();
                            }
                        }
                    },
                    RedisCommand::Set { key, value } => {
                        redis.lock().unwrap().set(key, value);
                        client.write_all(b"+OK\r\n").unwrap();
                    },
                    RedisCommand::Error { message } => {
                        client.write_all(b"-ERR ").unwrap();
                        client.write_all(message.as_bytes()).unwrap();
                        client.write_all(b"\r\n").unwrap();
                    }
                }

                // if Self::PING.contains(&command.command) {
                //     client.write_all(b"+PONG\r\n").unwrap();
                // } else if Self::ECHO.contains(&command.command) {
                //     client.write_all(b"+").unwrap();
                //     client.write_all(command.data.as_bytes()).unwrap();
                //     client.write_all(b"\r\n").unwrap();
                // } else {
                //     client.write_all(b"-ERR unknown command\r\n").unwrap();
                // }
            }
            println!("closing connection {}", client.peer_addr().unwrap());
        });
    }
}