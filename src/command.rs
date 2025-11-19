use crate::resp::Value;
use anyhow::{Result, Error};

#[derive(Debug, Clone, PartialEq)]
pub enum RedisCommand {
    Ping { message: Option<String> },
    Echo { message: String },
    Set { key: String, value: String, px: Option<u64> },
    Get { key: String },
    Info { section: String },
    ReplConf { subcommand: String, args: Vec<String> },
    PSync { replication_id: String, offset: i64 },
    Wait { num_replicas: usize, timeout: u64 },
    Error { message: String },
    None,
}

impl RedisCommand {
    pub fn from_resp(value: Value) -> Result<RedisCommand> {
        match value {
            Value::Array(items) => {
                if items.is_empty() {
                    return Ok(RedisCommand::None);
                }

                let command_name = match &items[0] {
                    Value::BulkString(s) => s.to_uppercase(),
                    _ => return Err(Error::msg("Invalid command format")),
                };

                match command_name.as_str() {
                    "PING" => {
                        let message = if items.len() > 1 {
                            match &items[1] {
                                Value::BulkString(s) => Some(s.clone()),
                                _ => None,
                            }
                        } else {
                            None
                        };
                        Ok(RedisCommand::Ping { message })
                    }
                    "ECHO" => {
                        if items.len() < 2 {
                            return Err(Error::msg("ERR wrong number of arguments for 'echo' command"));
                        }
                        let message = match &items[1] {
                            Value::BulkString(s) => s.clone(),
                            _ => return Err(Error::msg("Invalid argument for ECHO")),
                        };
                        Ok(RedisCommand::Echo { message })
                    }
                    "SET" => {
                        if items.len() < 3 {
                            return Err(Error::msg("ERR wrong number of arguments for 'set' command"));
                        }
                        let key = match &items[1] {
                            Value::BulkString(s) => s.clone(),
                            _ => return Err(Error::msg("Invalid key for SET")),
                        };
                        let value = match &items[2] {
                            Value::BulkString(s) => s.clone(),
                            _ => return Err(Error::msg("Invalid value for SET")),
                        };
                        
                        let mut px = None;
                        if items.len() > 3 {
                            if let Value::BulkString(opt) = &items[3] {
                                if opt.to_uppercase() == "PX" {
                                    if items.len() > 4 {
                                        if let Value::BulkString(ms) = &items[4] {
                                            if let Ok(ms_val) = ms.parse::<u64>() {
                                                px = Some(ms_val);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        
                        Ok(RedisCommand::Set { key, value, px })
                    }
                    "GET" => {
                        if items.len() < 2 {
                            return Err(Error::msg("ERR wrong number of arguments for 'get' command"));
                        }
                        let key = match &items[1] {
                            Value::BulkString(s) => s.clone(),
                            _ => return Err(Error::msg("Invalid key for GET")),
                        };
                        Ok(RedisCommand::Get { key })
                    }
                    "INFO" => {
                        let section = if items.len() > 1 {
                            match &items[1] {
                                Value::BulkString(s) => s.clone(),
                                _ => "default".to_string(),
                            }
                        } else {
                            "default".to_string()
                        };
                        Ok(RedisCommand::Info { section })
                    }
                    "REPLCONF" => {
                        if items.len() < 2 {
                            return Err(Error::msg("ERR wrong number of arguments for 'replconf' command"));
                        }
                        let subcommand = match &items[1] {
                            Value::BulkString(s) => s.clone(),
                            _ => return Err(Error::msg("Invalid subcommand for REPLCONF")),
                        };
                        let mut args = Vec::new();
                        for i in 2..items.len() {
                             match &items[i] {
                                Value::BulkString(s) => args.push(s.clone()),
                                _ => {},
                            }
                        }
                        Ok(RedisCommand::ReplConf { subcommand, args })
                    }
                    "PSYNC" => {
                        if items.len() < 3 {
                            return Err(Error::msg("ERR wrong number of arguments for 'psync' command"));
                        }
                        let replication_id = match &items[1] {
                            Value::BulkString(s) => s.clone(),
                            _ => return Err(Error::msg("Invalid replication_id for PSYNC")),
                        };
                        let offset = match &items[2] {
                            Value::BulkString(s) => s.parse::<i64>().unwrap_or(-1),
                            _ => -1,
                        };
                        Ok(RedisCommand::PSync { replication_id, offset })
                    }
                    "WAIT" => {
                        if items.len() < 3 {
                            return Err(Error::msg("ERR wrong number of arguments for 'wait' command"));
                        }
                        let num_replicas = match &items[1] {
                            Value::BulkString(s) => s.parse::<usize>()?,
                            _ => return Err(Error::msg("Invalid num_replicas for WAIT")),
                        };
                        let timeout = match &items[2] {
                            Value::BulkString(s) => s.parse::<u64>()?,
                            _ => return Err(Error::msg("Invalid timeout for WAIT")),
                        };
                        Ok(RedisCommand::Wait { num_replicas, timeout })
                    }
                    _ => Ok(RedisCommand::Error { message: format!("Unknown command: {}", command_name) }),
                }
            }
            _ => Ok(RedisCommand::None),
        }
    }
}
