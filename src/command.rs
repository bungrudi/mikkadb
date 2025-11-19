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
    XAdd { key: String, id: String, fields: Vec<(String, String)> },
    XRead {
        block: Option<u64>,
        streams: Vec<(String, String)>,
    },
    XRange {
        key: String,
        start: String,
        end: String,
    },
    Incr { key: String },
    Multi,
    Exec,
    Discard,
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
                    Value::BulkString(s) => {
                        println!("Received command: {}", s);
                        s.to_uppercase()
                    },
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
                    "XADD" => {
                        if items.len() < 4 {
                            return Err(Error::msg("ERR wrong number of arguments for 'xadd' command"));
                        }
                        let key = match &items[1] {
                            Value::BulkString(s) => s.clone(),
                            _ => return Err(Error::msg("Invalid key for XADD")),
                        };
                        let id = match &items[2] {
                            Value::BulkString(s) => s.clone(),
                            _ => return Err(Error::msg("Invalid ID for XADD")),
                        };
                        
                        let mut fields = Vec::new();
                        let mut i = 3;
                        while i < items.len() {
                            if i + 1 >= items.len() {
                                return Err(Error::msg("ERR wrong number of arguments for 'xadd' command"));
                            }
                            let field = match &items[i] {
                                Value::BulkString(s) => s.clone(),
                                _ => return Err(Error::msg("Invalid field for XADD")),
                            };
                            let value = match &items[i+1] {
                                Value::BulkString(s) => s.clone(),
                                _ => return Err(Error::msg("Invalid value for XADD")),
                            };
                            fields.push((field, value));
                            i += 2;
                        }
                        
                        Ok(RedisCommand::XAdd { key, id, fields })
                    }
                    "XREAD" => {
                        let mut block = None;
                        let mut streams_start_idx = 1;
                        
                        if items.len() > 1 {
                            if let Value::BulkString(s) = &items[1] {
                                if s.to_uppercase() == "BLOCK" {
                                    if items.len() < 3 {
                                        return Err(Error::msg("ERR syntax error"));
                                    }
                                    if let Value::BulkString(ms) = &items[2] {
                                        block = Some(ms.parse::<u64>()?);
                                    } else {
                                        return Err(Error::msg("ERR value is not an integer or out of range"));
                                    }
                                    streams_start_idx = 3;
                                }
                            }
                        }
                        
                        if items.len() <= streams_start_idx {
                             return Err(Error::msg("ERR wrong number of arguments for 'xread' command"));
                        }
                        
                        if let Value::BulkString(s) = &items[streams_start_idx] {
                            if s.to_uppercase() != "STREAMS" {
                                return Err(Error::msg("ERR syntax error")); 
                            }
                        } else {
                            return Err(Error::msg("ERR syntax error"));
                        }
                        
                        let args_count = items.len() - (streams_start_idx + 1);
                        if args_count % 2 != 0 {
                            return Err(Error::msg("ERR Unbalanced XREAD list of streams: for each stream key an ID or '$' must be specified."));
                        }
                        
                        let num_streams = args_count / 2;
                        let mut streams = Vec::new();
                        
                        for i in 0..num_streams {
                            let key_idx = streams_start_idx + 1 + i;
                            let id_idx = streams_start_idx + 1 + num_streams + i;
                            
                            let key = match &items[key_idx] {
                                Value::BulkString(s) => s.clone(),
                                _ => return Err(Error::msg("Invalid key")),
                            };
                            
                            let id = match &items[id_idx] {
                                Value::BulkString(s) => s.clone(),
                                _ => return Err(Error::msg("Invalid ID")),
                            };
                            
                            streams.push((key, id));
                        }
                        
                        Ok(RedisCommand::XRead { block, streams })
                    }
                    "XRANGE" => {
                        if items.len() < 4 {
                            return Err(Error::msg("ERR wrong number of arguments for 'xrange' command"));
                        }
                        let key = match &items[1] {
                            Value::BulkString(s) => s.clone(),
                            _ => return Err(Error::msg("Invalid key")),
                        };
                        let start = match &items[2] {
                            Value::BulkString(s) => s.clone(),
                            _ => return Err(Error::msg("Invalid start")),
                        };
                        let end = match &items[3] {
                            Value::BulkString(s) => s.clone(),
                            _ => return Err(Error::msg("Invalid end")),
                        };
                        
                        Ok(RedisCommand::XRange { key, start, end })
                    }
                    "INCR" => {
                        if items.len() < 2 {
                            return Err(Error::msg("ERR wrong number of arguments for 'incr' command"));
                        }
                        let key = match &items[1] {
                            Value::BulkString(s) => s.clone(),
                            _ => return Err(Error::msg("Invalid key for INCR")),
                        };
                        Ok(RedisCommand::Incr { key })
                    }
                    "MULTI" => Ok(RedisCommand::Multi),
                    "EXEC" => Ok(RedisCommand::Exec),
                    "DISCARD" => Ok(RedisCommand::Discard),
                    _ => Ok(RedisCommand::Error { message: format!("Unknown command: {}", command_name) }),
                }
            }
            _ => Ok(RedisCommand::None),
        }
    }
}
