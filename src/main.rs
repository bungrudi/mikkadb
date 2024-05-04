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
fn init_replica(config: &mut RedisConfig) {
    // TODO encapsulate startup logic..
    // as part of startup sequence..
    // if there is a replicaof host and port we should send PING
    // to master and wait for PONG.
    // how do we encapsulate the logic for this?

    // for now let's just code it here.
    // connect to remote host and port, send PING as RESP.
    // if we get PONG we are good to go.
    // followup with REPLCONF listening-port and REPLCONF capa psync2
    // the following sequence in separate thread..
    let config = config.clone();
    let handle = thread::spawn(move || {
        // wait for 1 sec to allow the main thread to start the listener..
        std::thread::sleep(std::time::Duration::from_secs(1));
        // TODO find a more elegant way to wait for the listener to start..
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
                            let replconf = format!("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${}\r\n{}\r\n",
                                                   config.port.len(), config.port);
                            stream.write(replconf.as_bytes()).unwrap();
                            let bytes_read = stream.read(&mut buffer).unwrap();
                            let response = std::str::from_utf8(&buffer[0..bytes_read]).unwrap();
                            if response == "+OK\r\n" {
                                println!("listening-port set");
                                // let replconf = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n$4\r\ncapa\r\n$3\r\neof\r\n";
                                let replconf = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
                                stream.write(replconf.as_bytes()).unwrap();
                                let bytes_read = stream.read(&mut buffer).unwrap();
                                let response = std::str::from_utf8(&buffer[0..bytes_read]).unwrap();
                                if response == "+OK\r\n" {
                                    println!("capa psync2 set");
                                    // send PSYNC ? -1km
                                    let psync = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
                                    stream.write(psync.as_bytes()).unwrap();
                                    let bytes_read = stream.read(&mut buffer).unwrap();
                                    println!("bytes read psync: {}", bytes_read);
                                    let response = std::str::from_utf8(&buffer[0..bytes_read]).unwrap();
                                    if response.to_lowercase().starts_with("+fullresync") {
                                        println!("psync -1 sent, got full resync");
                                    } else {
                                        eprintln!("error sending psync -1");
                                        std::process::exit(1);
                                    }
                                } else {
                                    eprintln!("error setting capa psync2");
                                    std::process::exit(1);
                                }
                            } else {
                                eprintln!("error setting listening-port");
                                std::process::exit(1);
                            }
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
    });

}
