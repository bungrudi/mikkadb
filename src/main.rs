use tokio::net::TcpListener;
use anyhow::Result;
use bytes::Bytes;
use std::sync::Arc;

mod resp;
mod db;
mod config;

#[tokio::main]
async fn main() -> Result<()> {
    let config = Arc::new(config::Config::parse());
    let addr = format!("127.0.0.1:{}", config.port);
    let listener = TcpListener::bind(&addr).await?;
    println!("Listening on {}", addr);

    let db = db::Db::new();

    if let config::ServerRole::Slave = config.role {
        let config = config.clone();
        tokio::spawn(async move {
            if let Err(e) = perform_handshake(config).await {
                eprintln!("Handshake error: {}", e);
            }
        });
    }

    loop {
        let (stream, _) = listener.accept().await?;
        let db = db.clone();
        let config = config.clone();
        tokio::spawn(async move {
            let mut handler = resp::RespHandler::new(stream);
            loop {
                let value = handler.read_value().await;
                match value {
                    Ok(Some(v)) => {
                        match v {
                            resp::Value::Array(a) => {
                                if let Some(resp::Value::BulkString(cmd)) = a.get(0) {
                                    match cmd.to_uppercase().as_str() {
                                        "PING" => {
                                            let _ = handler.write_value(resp::Value::SimpleString("PONG".to_string())).await;
                                        }
                                        "ECHO" => {
                                            if let Some(arg) = a.get(1) {
                                                let _ = handler.write_value(arg.clone()).await;
                                            }
                                        }
                                        "SET" => {
                                            if let (Some(resp::Value::BulkString(key)), Some(resp::Value::BulkString(value))) = (a.get(1), a.get(2)) {
                                                let mut px = None;
                                                if a.len() > 3 {
                                                    for i in 3..a.len() {
                                                        if let Some(resp::Value::BulkString(arg)) = a.get(i) {
                                                            if arg.to_uppercase() == "PX" {
                                                                if let Some(resp::Value::BulkString(ms_str)) = a.get(i + 1) {
                                                                    if let Ok(ms) = ms_str.parse::<u64>() {
                                                                        px = Some(ms);
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                                db.set(key.clone(), Bytes::from(value.clone()), px);
                                                let _ = handler.write_value(resp::Value::SimpleString("OK".to_string())).await;
                                            }
                                        }
                                        "GET" => {
                                            if let Some(resp::Value::BulkString(key)) = a.get(1) {
                                                match db.get(key) {
                                                    Some(value) => {
                                                        if let Ok(s) = String::from_utf8(value.to_vec()) {
                                                            let _ = handler.write_value(resp::Value::BulkString(s)).await;
                                                        }
                                                    }
                                                    None => {
                                                        let _ = handler.write_value(resp::Value::Null).await;
                                                    }
                                                }
                                            }
                                        }
                                        "INFO" => {
                                            let role = match config.role {
                                                config::ServerRole::Master => "master",
                                                config::ServerRole::Slave => "slave",
                                            };
                                            let info = format!(
                                                "role:{}\r\nmaster_replid:{}\r\nmaster_repl_offset:{}",
                                                role, config.master_replid, config.master_repl_offset
                                            );
                                            let _ = handler.write_value(resp::Value::BulkString(info)).await;
                                        }
                                        "REPLCONF" => {
                                            let _ = handler.write_value(resp::Value::SimpleString("OK".to_string())).await;
                                        }
                                        "PSYNC" => {
                                             let id = &config.master_replid;
                                             let offset = config.master_repl_offset;
                                             let response = format!("FULLRESYNC {} {}", id, offset);
                                             let _ = handler.write_value(resp::Value::SimpleString(response)).await;
                                        }
                                        _ => {}
                                    }
                                }
                            }
                            _ => {}
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

async fn perform_handshake(config: Arc<config::Config>) -> Result<()> {
    if let (Some(host), Some(port)) = (&config.master_host, config.master_port) {
        let addr = format!("{}:{}", host, port);
        let stream = tokio::net::TcpStream::connect(addr).await?;
        let mut handler = resp::RespHandler::new(stream);

        // 1. PING
        handler.write_value(resp::Value::Array(vec![resp::Value::BulkString("PING".to_string())])).await?;
        let _ = handler.read_value().await?;

        // 2. REPLCONF listening-port
        handler.write_value(resp::Value::Array(vec![
            resp::Value::BulkString("REPLCONF".to_string()),
            resp::Value::BulkString("listening-port".to_string()),
            resp::Value::BulkString(config.port.to_string()),
        ])).await?;
        let _ = handler.read_value().await?;

        // 3. REPLCONF capa psync2
        handler.write_value(resp::Value::Array(vec![
            resp::Value::BulkString("REPLCONF".to_string()),
            resp::Value::BulkString("capa".to_string()),
            resp::Value::BulkString("psync2".to_string()),
        ])).await?;
        let _ = handler.read_value().await?;

        // 4. PSYNC ? -1
        handler.write_value(resp::Value::Array(vec![
            resp::Value::BulkString("PSYNC".to_string()),
            resp::Value::BulkString("?".to_string()),
            resp::Value::BulkString("-1".to_string()),
        ])).await?;
        let _ = handler.read_value().await?;
        
        // Keep connection alive for future commands (not implemented yet)
        // For now we just drop the connection which might be enough for the handshake tests
        // but for full replication we need to keep reading.
        // Let's loop and read to keep it open.
         loop {
            let _ = handler.read_value().await?;
        }
    }
    Ok(())
}
