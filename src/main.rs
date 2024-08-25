mod client_handler;
mod resp;
mod redis;
use crate::redis::{Redis, RedisConfig, init_replica};

use std::io::Write;
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

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
        match args[i].as_str() {
            "--port" => {
                if i + 1 < args.len() {
                    config.port = args[i + 1].to_string();
                } else {
                    eprintln!("--port argument provided but no port number was given");
                    std::process::exit(1);
                }
            }
            "--addr" => {
                if i + 1 < args.len() {
                    config.addr = args[i + 1].to_string();
                } else {
                    eprintln!("--addr argument provided but no address was given");
                    std::process::exit(1);
                }
            }
            "--replicaof" => {
                if i + 2 < args.len() {
                    config.replicaof_host = Some(args[i + 1].to_string());
                    config.replicaof_port = Some(args[i + 2].to_string());
                } else if i + 1 < args.len() {
                    let replicaof: Vec<&str> = args[i + 1].split(" ").collect();
                    if replicaof.len() == 2 {
                        config.replicaof_host = Some(replicaof[0].to_string());
                        config.replicaof_port = Some(replicaof[1].to_string());
                    } else {
                        eprintln!("--replicaof argument provided but no host or port was given");
                        std::process::exit(1);
                    }
                } else {
                    eprintln!("--replicaof argument provided but no host or port was given");
                    std::process::exit(1);
                }
            }
            "--dir" => {
                if i + 1 < args.len() {
                    config.dir = args[i + 1].to_string();
                } else {
                    eprintln!("--dir argument provided but no directory was given");
                    std::process::exit(1);
                }
            }
            "--dbfilename" => {
                if i + 1 < args.len() {
                    config.dbfilename = args[i + 1].to_string();
                } else {
                    eprintln!("--dbfilename argument provided but no filename was given");
                    std::process::exit(1);
                }
            }
            _ => {}
        }
    }

    let redis = Arc::new(Mutex::new(Redis::new(config.clone())));

    // Parse RDB file
    redis.lock().unwrap().parse_rdb_file(&config.dir, &config.dbfilename).unwrap();

    let listener = TcpListener::bind(format!("{}:{}",config.addr,config.port)).unwrap();

    init_replica(&mut config, redis.clone());

    let mut handles = vec![];

    // if we are master and there are replicas connected, send replica_queue to replicas
    if config.replicaof_host.is_none() {

        let redis = Arc::clone(&redis);
        let handle = thread::spawn(move || {
            loop {
                {
                    let redis = redis.lock().unwrap();
                    {
                        let mut replica_queue = redis.replication.replica_queue.lock().unwrap();
                        while let Some(replica_command) = replica_queue.pop_front() {
                            let replicas = redis.replication.replicas.lock().unwrap();
                            println!("replicas size: {}", replicas.values().len());
                            for (key, replica) in &*replicas {
                                println!("sending replica command: {} \r\n to {} {}:{}", replica_command, key, &replica.host, &replica.port);
                                let mut stream = replica.stream.lock().unwrap();
                                let result = stream.write(replica_command.as_bytes());
                                if result.is_err() {
                                    eprintln!("error writing to replica: {}", result.err().unwrap());
                                }
                            }
                        }
                    }
                    // send "REPLCONF GETACK *" after each propagation
                    if redis.replication.should_send_getack() {
                        println!("sending GETACK to replicas");
                        for replica in redis.replication.get_replicas().values() {
                            let mut stream = replica.stream.lock().unwrap();
                            let result = stream.write(b"*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n");
                            if result.is_err() {
                                eprintln!("error writing to replica: {}", result.err().unwrap());
                            }
                        }
                        redis.replication.set_enqueue_getack(false);
                    }
                } // expecting the lock to be dropped here
                thread::sleep(Duration::from_millis(10));
                // println!("end of replica queue check loop");
            }
        });

        handles.push(handle);
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