use crate::resp::Value;
use anyhow::{Result, Error};
use bytes::Bytes;

#[derive(Debug, Clone, PartialEq)]
pub enum RedisCommand {
    Ping { message: Option<String> },
    Echo { message: String },
    /// SET command with zero-copy Bytes key and value
    Set { key: Bytes, value: Bytes, px: Option<u64> },
    /// GET command with zero-copy Bytes key
    Get { key: Bytes },
    Info { section: String },
    ReplConf { subcommand: String, args: Vec<String> },
    PSync { replication_id: String, offset: i64 },
    ConfigGet { parameter: String },
    Keys { pattern: String },
    Wait { num_replicas: usize, timeout: u64 },
    Subscribe { channels: Vec<String> },
    Publish { channel: String, message: String },
    Unsubscribe { channels: Vec<String> },
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
    /// INCR command with zero-copy Bytes key
    Incr { key: Bytes },
    Type { key: String },
    Multi,
    Exec,
    Discard,
    RPush { key: String, values: Vec<String> },
    LRange { key: String, start: i64, end: i64 },
    LPush { key: String, values: Vec<String> },
    LLen { key: String },
    LPop { key: String, count: Option<i64> },
    BLPop { keys: Vec<String>, timeout: f64 },
    InternalDisconnect,
    ZAdd { key: String, entries: Vec<(f64, String)> },
    ZRange { key: String, start: i64, end: i64, with_scores: bool },
    ZCard { key: String },
    ZScore { key: String, member: String },
    ZRank { key: String, member: String },
    ZRem { key: String, members: Vec<String> },
    Error { message: String },
    None,
}

impl RedisCommand {
    pub fn from_resp(value: Value) -> Result<RedisCommand> {
        match value {
            Value::Array(items) => {
                // println!("Parsing command with {} items", items.len());
                if items.is_empty() {
                    return Ok(RedisCommand::None);
                }

                let command_name = match items[0].to_uppercase_string() {
                    Ok(cmd) => cmd,
                    Err(_) => return Err(Error::msg("Invalid command format")),
                };

                match command_name.as_str() {
                    "PING" => {
                        let message = if items.len() > 1 {
                            match &items[1] {
                                Value::BulkString(_) => items[1].to_string().ok(),
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
                        let message = match items[1].to_string() {
                            Ok(s) => s,
                            _ => return Err(Error::msg("Invalid argument for ECHO")),
                        };
                        Ok(RedisCommand::Echo { message })
                    }
                    "SET" => {
                        if items.len() < 3 {
                            return Err(Error::msg("ERR wrong number of arguments for 'set' command"));
                        }
                        // Zero-copy: extract Bytes directly without String allocation
                        let key = match items[1].clone_bytes() {
                            Some(b) => b,
                            None => return Err(Error::msg("Invalid key for SET")),
                        };
                        let value = match items[2].clone_bytes() {
                            Some(b) => b,
                            None => return Err(Error::msg("Invalid value for SET")),
                        };
                        
                        let mut px = None;
                        if items.len() > 3 {
                            if let Ok(opt) = items[3].to_uppercase_string() {
                                if opt == "PX" {
                                    if items.len() > 4 {
                                        if let Ok(ms_str) = items[4].to_string() {
                                            if let Ok(ms_val) = ms_str.parse::<u64>() {
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
                        // Zero-copy: extract Bytes directly without String allocation
                        let key = match items[1].clone_bytes() {
                            Some(b) => b,
                            None => return Err(Error::msg("Invalid key for GET")),
                        };
                        Ok(RedisCommand::Get { key })
                    }
                    "INFO" => {
                        let section = if items.len() > 1 {
                            match items[1].to_string() {
                                Ok(s) => s,
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
                        let subcommand = match items[1].to_string() {
                            Ok(s) => s,
                            _ => return Err(Error::msg("Invalid subcommand for REPLCONF")),
                        };
                        let mut args = Vec::new();
                        for i in 2..items.len() {
                             match items[i].to_string() {
                                Ok(s) => args.push(s),
                                _ => {},
                            }
                        }
                        Ok(RedisCommand::ReplConf { subcommand, args })
                    }
                    "PSYNC" => {
                        if items.len() < 3 {
                            return Err(Error::msg("ERR wrong number of arguments for 'psync' command"));
                        }
                        let replication_id = match items[1].to_string() {
                            Ok(s) => s,
                            _ => return Err(Error::msg("Invalid replication_id for PSYNC")),
                        };
                        let offset = match items[2].to_string() {
                            Ok(s) => s.parse::<i64>().unwrap_or(-1),
                            _ => -1,
                        };
                        Ok(RedisCommand::PSync { replication_id, offset })
                    }
                    "CONFIG" => {
                        if items.len() < 3 {
                            return Err(Error::msg("ERR wrong number of arguments for 'config' command"));
                        }
                        let subcommand = match items[1].to_uppercase_string() {
                            Ok(s) => s,
                            _ => return Err(Error::msg("Invalid subcommand for CONFIG")),
                        };
                        match subcommand.as_str() {
                            "GET" => {
                                let parameter = match items[2].to_string() {
                                    Ok(s) => s,
                                    _ => return Err(Error::msg("Invalid parameter for CONFIG GET")),
                                };
                                Ok(RedisCommand::ConfigGet { parameter })
                            }
                            _ => Err(Error::msg("ERR Unsupported CONFIG subcommand")),
                        }
                    }
                    "KEYS" => {
                        if items.len() < 2 {
                            println!("KEYS: wrong number of arguments");
                            return Err(Error::msg("ERR wrong number of arguments for 'keys' command"));
                        }
                        let pattern = match items[1].to_string() {
                            Ok(s) => s,
                            _ => {
                                println!("KEYS: invalid pattern type: {:?}", items[1]);
                                return Err(Error::msg("Invalid pattern for KEYS"));
                            }
                        };
                        println!("KEYS: parsed pattern '{}'", pattern);
                        Ok(RedisCommand::Keys { pattern })
                    }
                    "WAIT" => {
                        if items.len() < 3 {
                            return Err(Error::msg("ERR wrong number of arguments for 'wait' command"));
                        }
                        let num_replicas = match items[1].to_string() {
                            Ok(s) => s.parse::<usize>()?,
                            _ => return Err(Error::msg("Invalid num_replicas for WAIT")),
                        };
                        let timeout = match items[2].to_string() {
                            Ok(s) => s.parse::<u64>()?,
                            _ => return Err(Error::msg("Invalid timeout for WAIT")),
                        };
                        Ok(RedisCommand::Wait { num_replicas, timeout })
                    }
                    "SUBSCRIBE" => {
                        let mut channels = Vec::new();
                        for item in &items[1..] {
                            if let Ok(s) = item.to_string() {
                                channels.push(s);
                            }
                        }
                        if channels.is_empty() {
                             return Err(Error::msg("ERR wrong number of arguments for 'subscribe' command"));
                        }
                        Ok(RedisCommand::Subscribe { channels })
                    }
                    "PUBLISH" => {
                        if items.len() != 3 {
                            return Err(Error::msg("ERR wrong number of arguments for 'publish' command"));
                        }
                        let channel = match items[1].to_string() {
                            Ok(s) => s,
                            _ => return Err(Error::msg("Invalid channel for PUBLISH")),
                        };
                        let message = match items[2].to_string() {
                            Ok(s) => s,
                            _ => return Err(Error::msg("Invalid message for PUBLISH")),
                        };
                        Ok(RedisCommand::Publish { channel, message })
                    }
                    "UNSUBSCRIBE" => {
                        let mut channels = Vec::new();
                        for item in &items[1..] {
                            if let Ok(s) = item.to_string() {
                                channels.push(s);
                            }
                        }
                        // Empty channels list is allowed (means unsubscribe from all)
                        Ok(RedisCommand::Unsubscribe { channels })
                    }
                    "XADD" => {
                        if items.len() < 4 {
                            return Err(Error::msg("ERR wrong number of arguments for 'xadd' command"));
                        }
                        let key = match items[1].to_string() {
                            Ok(s) => s,
                            _ => return Err(Error::msg("Invalid key for XADD")),
                        };
                        let id = match items[2].to_string() {
                            Ok(s) => s,
                            _ => return Err(Error::msg("Invalid ID for XADD")),
                        };

                        let mut fields = Vec::new();
                        let mut i = 3;
                        while i < items.len() {
                            if i + 1 >= items.len() {
                                return Err(Error::msg("ERR wrong number of arguments for 'xadd' command"));
                            }
                            let field = match items[i].to_string() {
                                Ok(s) => s,
                                _ => return Err(Error::msg("Invalid field for XADD")),
                            };
                            let value = match items[i+1].to_string() {
                                Ok(s) => s,
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
                            if let Ok(s) = items[1].to_uppercase_string() {
                                if s == "BLOCK" {
                                    if items.len() < 3 {
                                        return Err(Error::msg("ERR syntax error"));
                                    }
                                    if let Ok(ms_str) = items[2].to_string() {
                                        block = Some(ms_str.parse::<u64>()?);
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

                        if let Ok(s) = items[streams_start_idx].to_uppercase_string() {
                            if s != "STREAMS" {
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

                            let key = match items[key_idx].to_string() {
                                Ok(s) => s,
                                _ => return Err(Error::msg("Invalid key")),
                            };

                            let id = match items[id_idx].to_string() {
                                Ok(s) => s,
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
                        let key = match items[1].to_string() {
                            Ok(s) => s,
                            _ => return Err(Error::msg("Invalid key")),
                        };
                        let start = match items[2].to_string() {
                            Ok(s) => s,
                            _ => return Err(Error::msg("Invalid start")),
                        };
                        let end = match items[3].to_string() {
                            Ok(s) => s,
                            _ => return Err(Error::msg("Invalid end")),
                        };

                        Ok(RedisCommand::XRange { key, start, end })
                    }
                    "TYPE" => {
                        if items.len() < 2 {
                            return Err(Error::msg("ERR wrong number of arguments for 'type' command"));
                        }
                        let key = match items[1].to_string() {
                            Ok(s) => s,
                            _ => return Err(Error::msg("Invalid key for TYPE")),
                        };
                        Ok(RedisCommand::Type { key })
                    }
                    "INCR" => {
                        if items.len() < 2 {
                            return Err(Error::msg("ERR wrong number of arguments for 'incr' command"));
                        }
                        // Zero-copy: extract Bytes directly
                        let key = match items[1].clone_bytes() {
                            Some(b) => b,
                            None => return Err(Error::msg("Invalid key for INCR")),
                        };
                        Ok(RedisCommand::Incr { key })
                    }
                    "MULTI" => Ok(RedisCommand::Multi),
                    "EXEC" => Ok(RedisCommand::Exec),
                    "DISCARD" => Ok(RedisCommand::Discard),
                    "RPUSH" => {
                        if items.len() < 3 {
                            return Err(Error::msg("ERR wrong number of arguments for 'rpush' command"));
                        }
                        let key = match items[1].to_string() {
                            Ok(s) => s,
                            _ => return Err(Error::msg("Invalid key for RPUSH")),
                        };
                        let mut values = Vec::new();
                        for i in 2..items.len() {
                            match items[i].to_string() {
                                Ok(s) => values.push(s),
                                _ => return Err(Error::msg("Invalid value for RPUSH")),
                            }
                        }
                        Ok(RedisCommand::RPush { key, values })
                    }
                    "LRANGE" => {
                        if items.len() < 4 {
                            return Err(Error::msg("ERR wrong number of arguments for 'lrange' command"));
                        }
                        let key = match items[1].to_string() {
                            Ok(s) => s,
                            _ => return Err(Error::msg("Invalid key for LRANGE")),
                        };
                        let start = match items[2].to_string() {
                            Ok(s) => s.parse::<i64>()?,
                            _ => return Err(Error::msg("Invalid start for LRANGE")),
                        };
                        let end = match items[3].to_string() {
                            Ok(s) => s.parse::<i64>()?,
                            _ => return Err(Error::msg("Invalid end for LRANGE")),
                        };
                        Ok(RedisCommand::LRange { key, start, end })
                    }
                    "LPUSH" => {
                        if items.len() < 3 {
                            return Err(Error::msg("ERR wrong number of arguments for 'lpush' command"));
                        }
                        let key = match items[1].to_string() {
                            Ok(s) => s,
                            _ => return Err(Error::msg("Invalid key for LPUSH")),
                        };
                        let mut values = Vec::new();
                        for i in 2..items.len() {
                            match items[i].to_string() {
                                Ok(s) => values.push(s),
                                _ => return Err(Error::msg("Invalid value for LPUSH")),
                            }
                        }
                        Ok(RedisCommand::LPush { key, values })
                    }
                    "LLEN" => {
                        if items.len() < 2 {
                            return Err(Error::msg("ERR wrong number of arguments for 'llen' command"));
                        }
                        let key = match items[1].to_string() {
                            Ok(s) => s,
                            _ => return Err(Error::msg("Invalid key for LLEN")),
                        };
                        Ok(RedisCommand::LLen { key })
                    }
                    "LPOP" => {
                        if items.len() < 2 {
                            return Err(Error::msg("ERR wrong number of arguments for 'lpop' command"));
                        }
                        let key = match items[1].to_string() {
                            Ok(s) => s,
                            _ => return Err(Error::msg("Invalid key for LPOP")),
                        };
                        let count = if items.len() > 2 {
                            match items[2].to_string() {
                                Ok(s) => Some(s.parse::<i64>()?),
                                _ => return Err(Error::msg("Invalid count for LPOP")),
                            }
                        } else {
                            None
                        };
                        Ok(RedisCommand::LPop { key, count })
                    }
                    "BLPOP" => {
                        if items.len() < 3 {
                            return Err(Error::msg("ERR wrong number of arguments for 'blpop' command"));
                        }
                        let mut keys = Vec::new();
                        // The last argument is the timeout
                        for i in 1..items.len() - 1 {
                             match items[i].to_string() {
                                Ok(s) => keys.push(s),
                                _ => return Err(Error::msg("Invalid key for BLPOP")),
                            }
                        }
                        let timeout = match items[items.len() - 1].to_string() {
                            Ok(s) => s.parse::<f64>()?,
                            _ => return Err(Error::msg("Invalid timeout for BLPOP")),
                        };
                        Ok(RedisCommand::BLPop { keys, timeout })
                    }
                    "ZADD" => {
                        if items.len() < 4 {
                            return Err(Error::msg("ERR wrong number of arguments for 'zadd' command"));
                        }
                        let key = match items[1].to_string() {
                            Ok(s) => s,
                            _ => return Err(Error::msg("Invalid key for ZADD")),
                        };

                        let mut entries = Vec::new();
                        let mut i = 2;
                        while i < items.len() {
                            let score_str = match items[i].to_string() {
                                Ok(s) => s,
                                _ => return Err(Error::msg("Invalid score for ZADD")),
                            };

                            let score = match score_str.parse::<f64>() {
                                Ok(f) => f,
                                Err(_) => return Err(Error::msg("ERR value is not a valid float")),
                            };

                            if i + 1 >= items.len() {
                                return Err(Error::msg("ERR syntax error"));
                            }

                            let member = match items[i+1].to_string() {
                                Ok(s) => s,
                                _ => return Err(Error::msg("Invalid member for ZADD")),
                            };

                            entries.push((score, member));
                            i += 2;
                        }
                        Ok(RedisCommand::ZAdd { key, entries })
                    }
                    "ZRANGE" => {
                        if items.len() < 4 {
                             return Err(Error::msg("ERR wrong number of arguments for 'zrange' command"));
                        }
                        let key = match items[1].to_string() {
                            Ok(s) => s,
                            _ => return Err(Error::msg("Invalid key for ZRANGE")),
                        };
                        let start = match items[2].to_string() {
                            Ok(s) => s.parse::<i64>()?,
                            _ => return Err(Error::msg("Invalid start for ZRANGE")),
                        };
                        let end = match items[3].to_string() {
                            Ok(s) => s.parse::<i64>()?,
                            _ => return Err(Error::msg("Invalid end for ZRANGE")),
                        };

                        let mut with_scores = false;
                        if items.len() > 4 {
                            if let Ok(s) = items[4].to_uppercase_string() {
                                if s == "WITHSCORES" {
                                    with_scores = true;
                                }
                            }
                        }

                        Ok(RedisCommand::ZRange { key, start, end, with_scores })
                    }
                    "ZCARD" => {
                        if items.len() < 2 {
                            return Err(Error::msg("ERR wrong number of arguments for 'zcard' command"));
                        }
                        let key = match items[1].to_string() {
                            Ok(s) => s,
                            _ => return Err(Error::msg("Invalid key for ZCARD")),
                        };
                        Ok(RedisCommand::ZCard { key })
                    }
                    "ZSCORE" => {
                        if items.len() < 3 {
                            return Err(Error::msg("ERR wrong number of arguments for 'zscore' command"));
                        }
                        let key = match items[1].to_string() {
                            Ok(s) => s,
                            _ => return Err(Error::msg("Invalid key for ZSCORE")),
                        };
                        let member = match items[2].to_string() {
                            Ok(s) => s,
                            _ => return Err(Error::msg("Invalid member for ZSCORE")),
                        };
                        Ok(RedisCommand::ZScore { key, member })
                    }
                    "ZRANK" => {
                        if items.len() < 3 {
                            return Err(Error::msg("ERR wrong number of arguments for 'zrank' command"));
                        }
                        let key = match items[1].to_string() {
                            Ok(s) => s,
                            _ => return Err(Error::msg("Invalid key for ZRANK")),
                        };
                        let member = match items[2].to_string() {
                            Ok(s) => s,
                            _ => return Err(Error::msg("Invalid member for ZRANK")),
                        };
                        Ok(RedisCommand::ZRank { key, member })
                    }
                    "ZREM" => {
                        if items.len() < 3 {
                            return Err(Error::msg("ERR wrong number of arguments for 'zrem' command"));
                        }
                        let key = match items[1].to_string() {
                            Ok(s) => s,
                            _ => return Err(Error::msg("Invalid key for ZREM")),
                        };
                        let mut members = Vec::new();
                        for i in 2..items.len() {
                             match items[i].to_string() {
                                Ok(s) => members.push(s),
                                _ => return Err(Error::msg("Invalid member for ZREM")),
                            }
                        }
                        Ok(RedisCommand::ZRem { key, members })
                    }
                    _ => Ok(RedisCommand::Error { message: format!("Unknown command: {}", command_name) }),
                }
            }
            _ => Ok(RedisCommand::None),
        }
    }

    pub fn name(&self) -> &str {
        match self {
            RedisCommand::Ping { .. } => "ping",
            RedisCommand::Echo { .. } => "echo",
            RedisCommand::Set { .. } => "set",
            RedisCommand::Get { .. } => "get",
            RedisCommand::Info { .. } => "info",
            RedisCommand::ReplConf { .. } => "replconf",
            RedisCommand::PSync { .. } => "psync",
            RedisCommand::ConfigGet { .. } => "config",
            RedisCommand::Keys { .. } => "keys",
            RedisCommand::Wait { .. } => "wait",
            RedisCommand::Subscribe { .. } => "subscribe",
            RedisCommand::Publish { .. } => "publish",
            RedisCommand::Unsubscribe { .. } => "unsubscribe",
            RedisCommand::XAdd { .. } => "xadd",
            RedisCommand::XRead { .. } => "xread",
            RedisCommand::XRange { .. } => "xrange",
            RedisCommand::Incr { .. } => "incr",
            RedisCommand::Type { .. } => "type",
            RedisCommand::Multi => "multi",
            RedisCommand::Exec => "exec",
            RedisCommand::Discard => "discard",
            RedisCommand::RPush { .. } => "rpush",
            RedisCommand::LRange { .. } => "lrange",
            RedisCommand::LPush { .. } => "lpush",
            RedisCommand::LLen { .. } => "llen",
            RedisCommand::LPop { .. } => "lpop",
            RedisCommand::BLPop { .. } => "blpop",
            RedisCommand::InternalDisconnect => "internal_disconnect",
            RedisCommand::ZAdd { .. } => "zadd",
            RedisCommand::ZRange { .. } => "zrange",
            RedisCommand::ZCard { .. } => "zcard",
            RedisCommand::ZScore { .. } => "zscore",
            RedisCommand::ZRank { .. } => "zrank",
            RedisCommand::ZRem { .. } => "zrem",
            RedisCommand::Error { .. } => "error",
            RedisCommand::None => "none",
        }
    }
}
