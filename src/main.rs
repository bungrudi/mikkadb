mod client_handler;
mod resp;
mod redis;

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use crate::redis::{init_replica, Redis, RedisConfig};

/**
* This is an implementation of a key value store that immitates Redis.
*/
fn main() {

    println!("Logs from your program will appear here!");
    let args: Vec<String> = std::env::args().collect();
    let mut config = RedisConfig::new();
    // config.port = "6379"; // default port
    // config.addr = "0.0.0.0";
    // let mut replicaof_host: Option<String> = None;
    // let mut replicaof_port: Option<String> = None;

    for i in 0..args.len() {
        if args[i] == "--port" {
            if i + 1 < args.len() {
                config.port = args[i + 1].to_string();
            } else {
                eprintln!("--port argument provided but no port number was given");
                std::process::exit(1);
            }
        }
        if args[i] == "--addr" {
            if i + 1 < args.len() {
                config.addr = args[i + 1].to_string();
            } else {
                eprintln!("--addr argument provided but no address was given");
                std::process::exit(1);
            }
        }
        if args[i] == "--replicaof" {
            if i + 2 < args.len() {
                config.replicaof_host = Some(args[i + 1].to_string());
                config.replicaof_port = Some(args[i + 2].to_string());
            } else {
                eprintln!("--replicaof argument provided but no host or port was given");
                std::process::exit(1);
            }
        }
    }

    let redis = Arc::new(Mutex::new(redis::Redis::new(config.clone())));

    let listener = TcpListener::bind(format!("{}:{}",config.addr,config.port)).unwrap();

    init_replica(&mut config, &mut redis.lock().unwrap());

    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                println!("accepted new connection");
                let mut client_handler = client_handler::ClientHandler::new(_stream, redis.clone());
                client_handler.start();
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
