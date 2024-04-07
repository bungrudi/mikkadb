mod client_handler;

use std::io::{Read, Write};
// Uncomment this block to pass the first stage
use std::net::TcpListener;

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage

    let listener = TcpListener::bind("0.0.0.0:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                println!("accepted new connection");
                let mut client_handler = client_handler::ClientHandler::new(_stream);
                client_handler.start();
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
