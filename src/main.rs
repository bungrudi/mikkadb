mod client_handler;
mod resp;
mod redis;

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use crate::redis::RedisConfig;

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

    // TODO encapsulate startup logic..
    // as part of startup sequence..
    // if there is a replicaof host and port we should send PING
    // to master and wait for PONG.
    // how do we encapsulate the logic for this?

    // for now let's just code it here.
    // connect to remote host and port, send PING as RESP.
    // if we get PONG we are good to go.
    // if we don't get PONG we should exit.
    // if we get an error we should exit.
    // if we get a response that is not PONG we should exit.
    // if we get a response that is PONG we should continue.
    if let Some(replicaof_host) = &config.replicaof_host {
        if let Some(replicaof_port) = &config.replicaof_port {
            let replicaof_addr = format!("{}:{}", replicaof_host, replicaof_port);
            match std::net::TcpStream::connect(replicaof_addr) {
                Ok(mut stream) => {
                    let ping = "*1\r\n$4\r\nPING\r\n";
                    stream.write(ping.as_bytes()).unwrap();
                    let mut buffer = [0; 512];
                    let bytes_read = stream.read(&mut buffer).unwrap();
                    let response = std::str::from_utf8(&buffer[0..bytes_read]).unwrap();
                    if response == "+PONG\r\n" {
                        println!("replicaof host and port is valid");
                    } else {
                        eprintln!("replicaof host and port is invalid");
                        std::process::exit(1);
                    }
                }
                Err(e) => {
                    eprintln!("error connecting to replicaof host and port: {}", e);
                    std::process::exit(1);
                }
            }
        }
    }



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
