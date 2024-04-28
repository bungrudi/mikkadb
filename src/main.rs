mod client_handler;
mod resp;
mod redis;

use std::net::TcpListener;
use std::sync::{Arc, Mutex};
/**
* This is an implementation of a key value store that immitates Redis.
*/
fn main() {

    println!("Logs from your program will appear here!");
    let args: Vec<String> = std::env::args().collect();
    let mut port = "6379"; // default port
    let mut addr = "0.0.0.0";

    for i in 0..args.len() {
        if args[i] == "--port" {
            if i + 1 < args.len() {
                port = &args[i + 1];
            } else {
                eprintln!("--port argument provided but no port number was given");
                std::process::exit(1);
            }
        }
        if args[i] == "--addr" {
            if i + 1 < args.len() {
                addr = &args[i + 1];
            } else {
                eprintln!("--addr argument provided but no address was given");
                std::process::exit(1);
            }
        }
    }

    let redis = Arc::new(Mutex::new(redis::Redis::new()));

    let listener = TcpListener::bind(format!("{}:{}",addr,port)).unwrap();

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
