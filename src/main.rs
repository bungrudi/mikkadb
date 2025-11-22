use tokio::net::TcpListener;
use anyhow::Result;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use crate::config::Config;
use crate::engine::{Engine, CommandRequest};
use crate::command::RedisCommand;

mod resp;
mod db;
mod config;
mod command;
mod engine;
mod storage;
mod actor_store;
mod single_lock_store;

#[tokio::main]
async fn main() -> Result<()> {
    let config = Arc::new(Config::parse());
    println!("Listening on 127.0.0.1:{}", config.port);

    // Create the Engine actor
    let (tx, rx) = mpsc::channel(32);
    let mut engine = Engine::new(config.clone(), rx);
    tokio::spawn(async move {
        engine.run().await;
    });

    if let crate::config::ServerRole::Slave = config.role {
        if let (Some(host), Some(port)) = (&config.master_host, &config.master_port) {
            let host = host.clone();
            let port = port.clone();
            let listening_port = config.port.to_string();
            let tx = tx.clone();
            tokio::spawn(async move {
                if let Err(e) = perform_handshake(host, port.to_string(), listening_port, tx).await {
                    eprintln!("Handshake error: {}", e);
                }
            });
        }
    }

    let listener = TcpListener::bind(format!("127.0.0.1:{}", config.port)).await?;
    println!("Listening on 127.0.0.1:{}", config.port);
    
    let mut client_id_counter = 0;

    loop {
        let (stream, _) = listener.accept().await?;

        let tx = tx.clone();
        client_id_counter += 1;
        let client_id = client_id_counter;
        
        tokio::spawn(async move {
            let mut handler = resp::RespHandler::new(stream);
            let mut repl_rx: Option<mpsc::Receiver<crate::resp::Value>> = None;
            let (msg_tx, mut msg_rx) = mpsc::channel(32);
            
            loop {
                tokio::select! {
                    value = handler.read_value() => {
                        match value {
                            Ok(Some(v)) => {
                                match RedisCommand::from_resp(v) {
                                    Ok(command) => {
                                        let (resp_tx, resp_rx) = oneshot::channel();
                                        
                                        let mut replica_tx = None;
                                        if let RedisCommand::PSync { .. } = &command {
                                            let (tx, rx) = mpsc::channel(32);
                                            replica_tx = Some(tx);
                                            repl_rx = Some(rx);
                                        }
                                        
                                        let req = CommandRequest {
                                            client_id,
                                            command,
                                            response_tx: resp_tx,
                                            replica_tx,
                                            pub_sub_tx: Some(msg_tx.clone()),
                                        };
                                        
                                        if let Err(_) = tx.send(req).await {
                                            // println!("Engine receiver dropped");
                                            break;
                                        }
                                        match resp_rx.await {
                                            Ok(Ok(response)) => {
                                                if let crate::resp::Value::Error(msg) = &response {
                                                    if msg == "NO_REPLY" {
                                                        continue;
                                                    }
                                                }

                                                // if let crate::resp::Value::Array(_) = &response {
                                                //     println!("Sending Array response");
                                                // } else if let crate::resp::Value::SimpleString(s) = &response {
                                                //     println!("Sending SimpleString response: {}", s);
                                                // }
                                                let _ = handler.write_value(response).await;
                                            }
                                            Ok(Err(e)) => {
                                                // println!("Sending Error response from Engine: {}", e);
                                                let _ = handler.write_value(resp::Value::Error(format!("ERR {}", e))).await;
                                            }
                                            Err(_) => {
                                                // println!("Engine response sender dropped");
                                                break;
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        // println!("Sending Error response from Parser: {}", e);
                                        let _ = handler.write_value(resp::Value::Error(format!("ERR {}", e))).await;
                                    }
                                }
                            }
                            Ok(None) => break,
                            Err(_e) => {
                                // println!("Error: {}", _e);
                                break;
                            }
                        }
                    }
                    Some(cmd) = async {
                        if let Some(rx) = &mut repl_rx {
                            rx.recv().await
                        } else {
                            std::future::pending().await
                        }
                    } => {
                        let _ = handler.write_value(cmd).await;
                    }
                    Some(msg) = msg_rx.recv() => {
                        let _ = handler.write_value(msg).await;
                    }
                }
            }
            
            // Client disconnected
            let (resp_tx, _) = oneshot::channel();
            let req = CommandRequest {
                client_id,
                command: RedisCommand::InternalDisconnect,
                response_tx: resp_tx,
                replica_tx: None,
                pub_sub_tx: None,
            };
            let _ = tx.send(req).await;
        });
    }
}



async fn perform_handshake(master_host: String, master_port: String, listening_port: String, tx: mpsc::Sender<CommandRequest>) -> Result<()> {
    use tokio::net::TcpStream;
    use crate::resp::{RespHandler, Value};

    let stream = TcpStream::connect(format!("{}:{}", master_host, master_port)).await?;
    let mut handler = RespHandler::new(stream);
    
    // 1. PING
    handler.write_value(Value::Array(vec![Value::BulkString("PING".to_string())])).await?;
    let _ = handler.read_value().await?; // PONG

    // 2. REPLCONF listening-port
    handler.write_value(Value::Array(vec![
        Value::BulkString("REPLCONF".to_string()),
        Value::BulkString("listening-port".to_string()),
        Value::BulkString(listening_port),
    ])).await?;
    let _ = handler.read_value().await?; // OK

    // 3. REPLCONF capa psync2
    handler.write_value(Value::Array(vec![
        Value::BulkString("REPLCONF".to_string()),
        Value::BulkString("capa".to_string()),
        Value::BulkString("psync2".to_string()),
    ])).await?;
    let _ = handler.read_value().await?; // OK

    // 4. PSYNC ? -1
    handler.write_value(Value::Array(vec![
        Value::BulkString("PSYNC".to_string()),
        Value::BulkString("?".to_string()),
        Value::BulkString("-1".to_string()),
    ])).await?;
    
    // Expect FULLRESYNC
    let _ = handler.read_value().await?; 
    
    // Expect RDB file
    let _ = handler.read_rdb_file().await?;

    // Process commands from master
    loop {
        let value = handler.read_value().await?;
        match value {
            Some(v) => {
                eprintln!("[repl] received from master: {:?}", v);
                match RedisCommand::from_resp(v) {
                    Ok(command) => {
                        eprintln!("[repl] parsed replication command: {:?}", command);
                        if let RedisCommand::ReplConf { subcommand, .. } = &command {
                            if subcommand.to_uppercase() == "GETACK" {
                                let ack = Value::Array(vec![
                                    Value::BulkString("REPLCONF".to_string()),
                                    Value::BulkString("ACK".to_string()),
                                    Value::BulkString("0".to_string()),
                                ]);
                                eprintln!("[repl] sending ACK 0 to master");
                                handler.write_value(ack).await?;
                                continue;
                            }
                        }
                        // Execute command against Engine
                        let (resp_tx, resp_rx) = oneshot::channel();
                        let req = CommandRequest {
                            client_id: 0, // Internal/Replica ID
                            command,
                            response_tx: resp_tx,
                            replica_tx: None, // We are the replica, we don't propagate further
                            pub_sub_tx: None,
                        };

                        if let Err(_) = tx.send(req).await {
                            eprintln!("[repl] failed to send command to engine");
                            break;
                        }

                        // Wait for execution to finish
                        if let Err(_) = resp_rx.await {
                            eprintln!("[repl] engine dropped response channel");
                            break;
                        }
                    }
                    Err(e) => {
                        eprintln!("[repl] failed to parse replication command: {}", e);
                    }
                }
            }
            None => {
                eprintln!("[repl] master closed replication connection");
                break;
            }
        }
    }
    Ok(())
}
