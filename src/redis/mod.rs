pub mod commands;
pub mod config;
pub mod replica;
pub mod storage;
pub mod replication;
pub mod core;
pub mod utils;
pub mod rdb;
pub mod xread_parser;
pub mod xread_handler;

use std::sync::{Arc, Mutex};
pub use config::RedisConfig;
pub use commands::RedisCommand;
pub use core::Redis;
pub use utils::*;


// Error responses that signal retry behavior
// pub const XREAD_RETRY_PREFIX: &str = "XREAD_RETRY"; 

use std::net::TcpStream;
use std::thread;

/// This function is called when the Redis server is configured as a replica. It performs the following steps:
/// 1. Connects to the master server
/// 2. Sends a PING command to verify the connection
/// 3. Configures the replica with REPLCONF commands
/// 4. Initiates replication with the PSYNC command
///
/// # Arguments
///
/// * `config` - A mutable reference to the RedisConfig struct containing server configuration
/// * `redis` - An Arc<Mutex<Redis>> representing the shared Redis state
#[inline]
pub fn init_replica(config: &mut RedisConfig, redis: Arc<Mutex<Redis>>) {
    let config = config.clone();
    thread::sleep(std::time::Duration::from_millis(500));
    if let Some(replicaof_host) = &config.replicaof_host {
        if let Some(replicaof_port) = &config.replicaof_port {
            match connect_to_server(replicaof_host, replicaof_port) {
                Ok(mut stream) => {
                    let replconf_command = format!("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${}\r\n{}\r\n", config.port.len(), config.port);
                    let commands: Vec<(&str, Box<dyn Fn(&str, &mut TcpStream) -> bool>)> = vec![
                        ("*1\r\n$4\r\nPING\r\n", Box::new(|response: &str, _| response == "+PONG\r\n")),
                        (&replconf_command, Box::new(|response: &str, _| response == "+OK\r\n")),
                        ("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n", Box::new(|response: &str, _| response == "+OK\r\n")),
                    ];

                    let mut buffer = [0; 512];
                    for (command, validate) in commands {
                        #[cfg(debug_assertions)]
                        println!("sending command: {}", command);
                        if send_command(&mut stream, command).is_err() {
                            eprintln!("error executing command: {}", command);
                            std::process::exit(1);
                        }
                        #[cfg(debug_assertions)]
                        println!("reading response..");
                        let response = read_response(&mut stream, &mut buffer).unwrap();
                        #[cfg(debug_assertions)]
                        println!("response: {}", response);
                        if !validate(&response, &mut stream) {
                            eprintln!("unexpected response: {}", response);
                            std::process::exit(1);
                        }
                    }
                    // send psync, and then hand over to the event loop.
                    if send_command(&mut stream, "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n").is_err() {
                        eprintln!("error executing PSYNC command");
                        std::process::exit(1);
                    }
                    #[cfg(debug_assertions)]
                    println!("psync sent");
                    // Read fullresync and RDB file, and no more.
                    // leave the rest to the event loop.

                    read_until_end_of_rdb(&mut stream, &mut buffer);

                    let mut client_handler = crate::client_handler::ClientHandler::new(stream, redis.clone());
                    client_handler.start();
                    #[cfg(debug_assertions)]
                    println!("replicaof host and port is valid");
                }
                Err(e) => {
                    eprintln!("error connecting to replicaof host and port: {}", e);
                    std::process::exit(1);
                }
            }
        }
    }
}
