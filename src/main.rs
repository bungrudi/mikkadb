mod client_handler;
mod resp;
mod redis;

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::thread;
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

    init_replica(&mut config);

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

#[inline]
fn connect_to_replica(host: &str, port: &str) -> std::io::Result<std::net::TcpStream> {
    let replicaof_addr = format!("{}:{}", host, port);
    std::net::TcpStream::connect(replicaof_addr)
}

#[inline]
fn send_command(stream: &mut std::net::TcpStream, command: &str) -> std::io::Result<usize> {
    stream.write(command.as_bytes())
}

#[inline]
fn read_response(stream: &mut std::net::TcpStream, buffer: &mut [u8; 512]) -> std::io::Result<String> {
    let bytes_read = stream.read(buffer)?;
    Ok(std::str::from_utf8(&buffer[0..bytes_read]).unwrap().to_string())
}

#[inline]
fn init_replica(config: &mut RedisConfig) {
    // as part of startup sequence..
    // if there is a replicaof host and port then this instance is a replica.

    // for now let's just code it here.
    // connect to remote host and port, send PING as RESP.
    // (a) sends PING command - wait for +PONG
    // (b) sends REPLCONF listening-port <PORT> - wait for +OK
    // (c) sends REPLCONF capa eof capa psync2 - wait for +OK
    // (d) sends PSYNC ? -1 - wait for +fullresync
    // the following sequence in separate thread..
    let config = config.clone();
    let handle = thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_secs(1));
        if let Some(replicaof_host) = &config.replicaof_host {
            if let Some(replicaof_port) = &config.replicaof_port {
                match connect_to_replica(replicaof_host, replicaof_port) {
                    Ok(mut stream) => {
                        let replconf_command = format!("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${}\r\n{}\r\n", config.port.len(), config.port);
                        let commands: Vec<(&str, Box<dyn Fn(&str) -> bool>)> = vec![
                            ("*1\r\n$4\r\nPING\r\n", Box::new(|response: &str| response == "+PONG\r\n")),
                            (&replconf_command, Box::new(|response: &str| response == "+OK\r\n")),
                            ("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n", Box::new(|response: &str| response == "+OK\r\n")),
                            ("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n", Box::new(|response: &str| response.to_lowercase().starts_with("+fullresync")))
                        ];

                        let mut buffer = [0; 512];
                        for (command, validate) in commands {
                            println!("sending command: {}", command);
                            if send_command(&mut stream, command).is_err() {
                                eprintln!("error executing command: {}", command);
                                std::process::exit(1);
                            }
                            println!("reading response..");
                            let response = read_response(&mut stream, &mut buffer).unwrap();
                            println!("response: {}", response);
                            if !validate(&response) {
                                eprintln!("unexpected response: {}", response);
                                std::process::exit(1);
                            }
                        }
                        println!("replicaof host and port is valid");
                    }
                    Err(e) => {
                        eprintln!("error connecting to replicaof host and port: {}", e);
                        std::process::exit(1);
                    }
                }
            }
        }
    });
}
