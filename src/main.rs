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
