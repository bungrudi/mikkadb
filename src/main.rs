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
            tokio::spawn(async move {
                if let Err(e) = perform_handshake(host, port.to_string(), listening_port).await {
                    eprintln!("Handshake error: {}", e);
                }
            });
        }
    }

    let listener = TcpListener::bind(format!("127.0.0.1:{}", config.port)).await?;

    loop {
        let (stream, _) = listener.accept().await?;
        let tx = tx.clone();
        tokio::spawn(async move {
            let mut handler = resp::RespHandler::new(stream);
            loop {
                let value = handler.read_value().await;
                match value {
                    Ok(Some(v)) => {
                        match RedisCommand::from_resp(v) {
                            Ok(command) => {
                                let (resp_tx, resp_rx) = oneshot::channel();
                                let req = CommandRequest {
                                    command,
                                    response_tx: resp_tx,
                                };
                                if let Err(_) = tx.send(req).await {
                                    println!("Engine receiver dropped");
                                    break;
                                }
                                match resp_rx.await {
                                    Ok(Ok(response)) => {
                                        let _ = handler.write_value(response).await;
                                    }
                                    Ok(Err(e)) => {
                                        let _ = handler.write_value(resp::Value::SimpleString(format!("ERR {}", e))).await;
                                    }
                                    Err(_) => {
                                        println!("Engine response sender dropped");
                                        break;
                                    }
                                }
                            }
                            Err(e) => {
                                let _ = handler.write_value(resp::Value::SimpleString(format!("ERR {}", e))).await;
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
        });
    }
}

async fn perform_handshake(master_host: String, master_port: String, listening_port: String) -> Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let mut stream = TcpStream::connect(format!("{}:{}", master_host, master_port)).await?;
    
    // 1. PING
    stream.write_all(b"*1\r\n$4\r\nPING\r\n").await?;
    let mut buf = [0; 1024];
    let _ = stream.read(&mut buf).await?; // Expect +PONG

    // 2. REPLCONF listening-port
    let cmd = format!("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${}\r\n{}\r\n", listening_port.len(), listening_port);
    stream.write_all(cmd.as_bytes()).await?;
    let _ = stream.read(&mut buf).await?; // Expect +OK

    // 3. REPLCONF capa psync2
    stream.write_all(b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n").await?;
    let _ = stream.read(&mut buf).await?; // Expect +OK

    // 4. PSYNC ? -1
    stream.write_all(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n").await?;
    let _ = stream.read(&mut buf).await?; // Expect +FULLRESYNC...

    // Keep connection alive
    loop {
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            break;
        }
    }
    Ok(())
}
