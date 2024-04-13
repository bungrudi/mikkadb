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

    let redis = Arc::new(Mutex::new(redis::Redis::new()));

    let listener = TcpListener::bind("0.0.0.0:6379").unwrap();

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
