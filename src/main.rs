use tokio::net::TcpListener;
use anyhow::Result;
use bytes::Bytes;

mod resp;
mod db;

#[tokio::main]
async fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    println!("Listening on 127.0.0.1:6379");

    let db = db::Db::new();

    loop {
        let (stream, _) = listener.accept().await?;
        let db = db.clone();
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
                                                db.set(key.clone(), Bytes::from(value.clone()));
                                                let _ = handler.write_value(resp::Value::SimpleString("OK".to_string())).await;
                                            }
                                        }
                                        "GET" => {
                                            if let Some(resp::Value::BulkString(key)) = a.get(1) {
                                                match db.get(key) {
                                                    Some(value) => {
                                                        // Convert Bytes to String for BulkString
                                                        // Assuming valid UTF-8 for now as Value::BulkString expects String
                                                        // TODO: Value::BulkString should probably hold Bytes
                                                        if let Ok(s) = String::from_utf8(value.to_vec()) {
                                                            let _ = handler.write_value(resp::Value::BulkString(s)).await;
                                                        } else {
                                                            // Handle non-utf8?
                                                        }
                                                    }
                                                    None => {
                                                        let _ = handler.write_value(resp::Value::Null).await;
                                                    }
                                                }
                                            }
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
