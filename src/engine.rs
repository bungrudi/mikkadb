use crate::command::RedisCommand;
use crate::db::Db;
use crate::resp::Value;
use crate::config::{Config, ServerRole};
use tokio::sync::{mpsc, oneshot};
use anyhow::{Result, Error};
use std::sync::Arc;
use bytes::Bytes;

pub struct CommandRequest {
    pub command: RedisCommand,
    pub response_tx: oneshot::Sender<Result<Value>>,
    pub replica_tx: Option<mpsc::Sender<Value>>,
}

pub struct Engine {
    db: Db,
    config: Arc<Config>,
    rx: mpsc::Receiver<CommandRequest>,
    replicas: Vec<mpsc::Sender<Value>>,
}

impl Engine {
    pub fn new(config: Arc<Config>, rx: mpsc::Receiver<CommandRequest>) -> Self {
        Engine {
            db: Db::new(),
            config,
            rx,
            replicas: Vec::new(),
        }
    }

    pub async fn run(&mut self) {
        while let Some(req) = self.rx.recv().await {
            let response = self.execute_command(&req).await;
            let _ = req.response_tx.send(response);
        }
    }

    async fn execute_command(&mut self, req: &CommandRequest) -> Result<Value> {
        match &req.command {
            RedisCommand::Ping { message } => {
                match message {
                    Some(msg) => Ok(Value::BulkString(msg.clone())),
                    None => Ok(Value::SimpleString("PONG".to_string())),
                }
            }
            RedisCommand::Echo { message } => Ok(Value::BulkString(message.clone())),
            RedisCommand::Set { key, value, px } => {
                self.db.set(key.clone(), bytes::Bytes::from(value.clone()), *px);
                
                // Propagate SET command to replicas
                // We need to reconstruct the command as Value
                // SET key value [PX px]
                let mut args = vec![
                    Value::BulkString("SET".to_string()),
                    Value::BulkString(key.clone()),
                    Value::BulkString(value.clone()),
                ];
                
                if let Some(ms) = px {
                    args.push(Value::BulkString("PX".to_string()));
                    args.push(Value::BulkString(ms.to_string()));
                }
                
                let cmd_value = Value::Array(args);
                self.propagate_command(cmd_value).await;
                
                Ok(Value::SimpleString("OK".to_string()))
            }
            RedisCommand::Get { key } => {
                match self.db.get(key) {
                    Some(value) => Ok(Value::BulkString(String::from_utf8_lossy(&value).to_string())),
                    None => Ok(Value::Null),
                }
            }
            RedisCommand::Info { section: _ } => {
                let role = match self.config.role {
                    crate::config::ServerRole::Master => "master",
                    crate::config::ServerRole::Slave => "slave",
                };
                let info = format!(
                    "role:{}\r\nmaster_replid:{}\r\nmaster_repl_offset:{}",
                    role, self.config.master_replid, self.config.master_repl_offset
                );
                Ok(Value::BulkString(info))
            }
            RedisCommand::ReplConf { .. } => {
                // TODO: Handle capabilities
                Ok(Value::SimpleString("OK".to_string()))
            }
            RedisCommand::PSync { replication_id: _, offset: _ } => {
                // Register replica if channel provided
                if let Some(tx) = &req.replica_tx {
                    self.replicas.push(tx.clone());
                }
                
                let response = format!("FULLRESYNC {} {}", self.config.master_replid, self.config.master_repl_offset);
                // Send FULLRESYNC first
                // let _ = req.response_tx.send(Ok(Value::SimpleString(response)));
                
                // Then send RDB file
                // Empty RDB file in hex
                let empty_rdb_hex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
                let empty_rdb = hex::decode(empty_rdb_hex).unwrap();
                
                Ok(Value::Multiple(vec![
                    Value::SimpleString(response),
                    Value::RdbFile(empty_rdb)
                ]))
            }
            RedisCommand::Error { message } => {
                Ok(Value::SimpleString(format!("ERR {}", message))) // Or Error type
            }
            RedisCommand::None => {
                Ok(Value::Null)
            }
        }
    }
    
    async fn propagate_command(&mut self, cmd: Value) {
        // We need to remove dead replicas
        // But retain is synchronous and send is async.
        // We can iterate and collect dead indices?
        // Or just ignore errors for now (lazy cleanup).
        // For Stage 111, we just need to send.
        
        for replica in &self.replicas {
            let _ = replica.send(cmd.clone()).await;
        }
    }
}
