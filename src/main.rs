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
                                        };
                                        
                                        if let Err(_) = tx.send(req).await {
                                            println!("Engine receiver dropped");
                                            break;
                                        }
                                        match resp_rx.await {
                                            Ok(Ok(response)) => {
                                                if let crate::resp::Value::Array(_) = &response {
                                                    println!("Sending Array response");
                                                } else if let crate::resp::Value::SimpleString(s) = &response {
                                                    println!("Sending SimpleString response: {}", s);
                                                }
                                                let _ = handler.write_value(response).await;
                                            }
                                            Ok(Err(e)) => {
                                                println!("Sending Error response from Engine: {}", e);
                                                let _ = handler.write_value(resp::Value::Error(format!("ERR {}", e))).await;
                                            }
                                            Err(_) => {
                                                println!("Engine response sender dropped");
                                                break;
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        println!("Sending Error response from Parser: {}", e);
                                        let _ = handler.write_value(resp::Value::Error(format!("ERR {}", e))).await;
                                    }
                                }
                            }
                            Ok(None) => break,
                            Err(e) => {
                                println!("Error: {}", e);
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
                }
            }
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
        if let Some(v) = value {
            if let Ok(command) = RedisCommand::from_resp(v) {
                // Execute command against Engine
                let (resp_tx, resp_rx) = oneshot::channel();
                let req = CommandRequest {
                    client_id: 0, // Internal/Replica ID
                    command,
                    response_tx: resp_tx,
                    replica_tx: None, // We are the replica, we don't propagate further
                };
                
                if let Err(_) = tx.send(req).await {
                    break;
                }
                
                // Wait for execution to finish
                let _ = resp_rx.await;
            }
        } else {
            break;
        }
    }
    Ok(())
}
