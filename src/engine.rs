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
}

pub struct Engine {
    db: Db,
    config: Arc<Config>,
    rx: mpsc::Receiver<CommandRequest>,
}

impl Engine {
    pub fn new(config: Arc<Config>, rx: mpsc::Receiver<CommandRequest>) -> Self {
        Engine {
            db: Db::new(),
            config,
            rx,
        }
    }

    pub async fn run(&mut self) {
        while let Some(req) = self.rx.recv().await {
            let response = self.execute_command(req.command).await;
            let _ = req.response_tx.send(response);
        }
    }

    async fn execute_command(&mut self, command: RedisCommand) -> Result<Value> {
        match command {
            RedisCommand::Ping { message } => {
                match message {
                    Some(msg) => Ok(Value::BulkString(msg)),
                    None => Ok(Value::SimpleString("PONG".to_string())),
                }
            }
            RedisCommand::Echo { message } => {
                Ok(Value::BulkString(message))
            }
            RedisCommand::Set { key, value, px } => {
                self.db.set(key, Bytes::from(value), px);
                Ok(Value::SimpleString("OK".to_string()))
            }
            RedisCommand::Get { key } => {
                match self.db.get(&key) {
                    Some(value) => {
                        if let Ok(s) = String::from_utf8(value.to_vec()) {
                            Ok(Value::BulkString(s))
                        } else {
                            Err(Error::msg("Value is not valid UTF-8"))
                        }
                    }
                    None => Ok(Value::Null),
                }
            }
            RedisCommand::Info { section: _ } => {
                let role_str = match self.config.role {
                    ServerRole::Master => "master",
                    ServerRole::Slave => "slave",
                };
                
                let info = format!(
                    "role:{}\r\nmaster_replid:{}\r\nmaster_repl_offset:{}",
                    role_str,
                    self.config.master_replid,
                    self.config.master_repl_offset
                );
                Ok(Value::BulkString(info))
            }
            RedisCommand::ReplConf { subcommand: _, args: _ } => {
                // For now, just return OK as Master
                Ok(Value::SimpleString("OK".to_string()))
            }
            RedisCommand::PSync { replication_id: _, offset: _ } => {
                let response = format!("FULLRESYNC {} {}", self.config.master_replid, self.config.master_repl_offset);
                Ok(Value::SimpleString(response))
            }
            RedisCommand::Error { message } => {
                Ok(Value::SimpleString(format!("ERR {}", message))) // Or Error type
            }
            RedisCommand::None => {
                Ok(Value::Null)
            }
        }
    }
}
