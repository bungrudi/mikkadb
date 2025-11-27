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
    /// DEL command - delete one or more keys
    Del { keys: Vec<Bytes> },
    /// EXISTS command - check if one or more keys exist
    Exists { keys: Vec<Bytes> },
    /// EXPIRE command - set key expiration in seconds
    Expire { key: Bytes, seconds: i64 },
    /// PEXPIRE command - set key expiration in milliseconds
    PExpire { key: Bytes, milliseconds: i64 },
    /// TTL command - get key time-to-live in seconds
    Ttl { key: Bytes },
    /// PTTL command - get key time-to-live in milliseconds
    PTtl { key: Bytes },
    /// PERSIST command - remove expiration from key
    Persist { key: Bytes },
    /// DECR command - decrement integer value by 1
    Decr { key: Bytes },
    /// DECRBY command - decrement integer value by specified amount
    DecrBy { key: Bytes, decrement: i64 },
    /// INCRBY command - increment integer value by specified amount
    IncrBy { key: Bytes, increment: i64 },
    /// APPEND command - append value to string
    Append { key: Bytes, value: Bytes },
    /// STRLEN command - get string length
    StrLen { key: Bytes },
    /// SETNX command - set if not exists
    SetNx { key: Bytes, value: Bytes },
    /// SETEX command - set with expiration in seconds
    SetEx { key: Bytes, seconds: i64, value: Bytes },
    /// RENAME command - rename a key
    Rename { key: Bytes, newkey: Bytes },
    /// HSET command - set hash fields
    HSet { key: Bytes, fields: Vec<(Bytes, Bytes)> },
    /// HGET command - get hash field
    HGet { key: Bytes, field: Bytes },
    /// HMGET command - get multiple hash fields
    HMGet { key: Bytes, fields: Vec<Bytes> },
    /// HGETALL command - get all fields and values
    HGetAll { key: Bytes },
    /// HDEL command - delete hash fields
    HDel { key: Bytes, fields: Vec<Bytes> },
    /// HEXISTS command - check if hash field exists
    HExists { key: Bytes, field: Bytes },
    /// HKEYS command - get all hash field names
    HKeys { key: Bytes },
    /// HVALS command - get all hash field values
    HVals { key: Bytes },
    /// HLEN command - get number of hash fields
    HLen { key: Bytes },
    /// HINCRBY command - increment hash field by integer
    HIncrBy { key: Bytes, field: Bytes, increment: i64 },
    /// HSETNX command - set hash field only if not exists
    HSetNx { key: Bytes, field: Bytes, value: Bytes },
    /// SADD command - add members to set
    SAdd { key: Bytes, members: Vec<Bytes> },
    /// SREM command - remove members from set
    SRem { key: Bytes, members: Vec<Bytes> },
    /// SMEMBERS command - get all set members
    SMembers { key: Bytes },
    /// SISMEMBER command - check if member exists in set
    SIsMember { key: Bytes, member: Bytes },
    /// SCARD command - get set cardinality (size)
    SCard { key: Bytes },
    /// SPOP command - remove and return random members
    SPop { key: Bytes, count: Option<usize> },
    /// SRANDMEMBER command - get random members without removing
    SRandMember { key: Bytes, count: Option<i64> },
    /// MGET command - get multiple keys at once
    MGet { keys: Vec<Bytes> },
    /// MSET command - set multiple key-value pairs at once
    MSet { pairs: Vec<(Bytes, Bytes)> },
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
                    "DEL" => {
                        if items.len() < 2 {
                            return Err(Error::msg("ERR wrong number of arguments for 'del' command"));
                        }
                        let mut keys = Vec::new();
                        for i in 1..items.len() {
                            match items[i].clone_bytes() {
                                Some(b) => keys.push(b),
                                None => return Err(Error::msg("Invalid key for DEL")),
                            }
                        }
                        Ok(RedisCommand::Del { keys })
                    }
                    "EXISTS" => {
                        if items.len() < 2 {
                            return Err(Error::msg("ERR wrong number of arguments for 'exists' command"));
                        }
                        let mut keys = Vec::new();
                        for i in 1..items.len() {
                            match items[i].clone_bytes() {
                                Some(b) => keys.push(b),
                                None => return Err(Error::msg("Invalid key for EXISTS")),
                            }
                        }
                        Ok(RedisCommand::Exists { keys })
                    }
                    "EXPIRE" => {
                        if items.len() < 3 {
                            return Err(Error::msg("ERR wrong number of arguments for 'expire' command"));
                        }
                        let key = match items[1].clone_bytes() {
                            Some(b) => b,
                            None => return Err(Error::msg("Invalid key for EXPIRE")),
                        };
                        let seconds = match items[2].to_string() {
                            Ok(s) => s.parse::<i64>()?,
                            _ => return Err(Error::msg("Invalid seconds for EXPIRE")),
                        };
                        Ok(RedisCommand::Expire { key, seconds })
                    }
                    "PEXPIRE" => {
                        if items.len() < 3 {
                            return Err(Error::msg("ERR wrong number of arguments for 'pexpire' command"));
                        }
                        let key = match items[1].clone_bytes() {
                            Some(b) => b,
                            None => return Err(Error::msg("Invalid key for PEXPIRE")),
                        };
                        let milliseconds = match items[2].to_string() {
                            Ok(s) => s.parse::<i64>()?,
                            _ => return Err(Error::msg("Invalid milliseconds for PEXPIRE")),
                        };
                        Ok(RedisCommand::PExpire { key, milliseconds })
                    }
                    "TTL" => {
                        if items.len() < 2 {
                            return Err(Error::msg("ERR wrong number of arguments for 'ttl' command"));
                        }
                        let key = match items[1].clone_bytes() {
                            Some(b) => b,
                            None => return Err(Error::msg("Invalid key for TTL")),
                        };
                        Ok(RedisCommand::Ttl { key })
                    }
                    "PTTL" => {
                        if items.len() < 2 {
                            return Err(Error::msg("ERR wrong number of arguments for 'pttl' command"));
                        }
                        let key = match items[1].clone_bytes() {
                            Some(b) => b,
                            None => return Err(Error::msg("Invalid key for PTTL")),
                        };
                        Ok(RedisCommand::PTtl { key })
                    }
                    "PERSIST" => {
                        if items.len() < 2 {
                            return Err(Error::msg("ERR wrong number of arguments for 'persist' command"));
                        }
                        let key = match items[1].clone_bytes() {
                            Some(b) => b,
                            None => return Err(Error::msg("Invalid key for PERSIST")),
                        };
                        Ok(RedisCommand::Persist { key })
                    }
                    "DECR" => {
                        if items.len() < 2 {
                            return Err(Error::msg("ERR wrong number of arguments for 'decr' command"));
                        }
                        let key = match items[1].clone_bytes() {
                            Some(b) => b,
                            None => return Err(Error::msg("Invalid key for DECR")),
                        };
                        Ok(RedisCommand::Decr { key })
                    }
                    "DECRBY" => {
                        if items.len() < 3 {
                            return Err(Error::msg("ERR wrong number of arguments for 'decrby' command"));
                        }
                        let key = match items[1].clone_bytes() {
                            Some(b) => b,
                            None => return Err(Error::msg("Invalid key for DECRBY")),
                        };
                        let decrement = match items[2].to_string() {
                            Ok(s) => s.parse::<i64>()?,
                            _ => return Err(Error::msg("Invalid decrement for DECRBY")),
                        };
                        Ok(RedisCommand::DecrBy { key, decrement })
                    }
                    "INCRBY" => {
                        if items.len() < 3 {
                            return Err(Error::msg("ERR wrong number of arguments for 'incrby' command"));
                        }
                        let key = match items[1].clone_bytes() {
                            Some(b) => b,
                            None => return Err(Error::msg("Invalid key for INCRBY")),
                        };
                        let increment = match items[2].to_string() {
                            Ok(s) => s.parse::<i64>()?,
                            _ => return Err(Error::msg("Invalid increment for INCRBY")),
                        };
                        Ok(RedisCommand::IncrBy { key, increment })
                    }
                    "APPEND" => {
                        if items.len() < 3 {
                            return Err(Error::msg("ERR wrong number of arguments for 'append' command"));
                        }
                        let key = match items[1].clone_bytes() {
                            Some(b) => b,
                            None => return Err(Error::msg("Invalid key for APPEND")),
                        };
                        let value = match items[2].clone_bytes() {
                            Some(b) => b,
                            None => return Err(Error::msg("Invalid value for APPEND")),
                        };
                        Ok(RedisCommand::Append { key, value })
                    }
                    "STRLEN" => {
                        if items.len() < 2 {
                            return Err(Error::msg("ERR wrong number of arguments for 'strlen' command"));
                        }
                        let key = match items[1].clone_bytes() {
                            Some(b) => b,
                            None => return Err(Error::msg("Invalid key for STRLEN")),
                        };
                        Ok(RedisCommand::StrLen { key })
                    }
                    "SETNX" => {
                        if items.len() < 3 {
                            return Err(Error::msg("ERR wrong number of arguments for 'setnx' command"));
                        }
                        let key = match items[1].clone_bytes() {
                            Some(b) => b,
                            None => return Err(Error::msg("Invalid key for SETNX")),
                        };
                        let value = match items[2].clone_bytes() {
                            Some(b) => b,
                            None => return Err(Error::msg("Invalid value for SETNX")),
                        };
                        Ok(RedisCommand::SetNx { key, value })
                    }
                    "SETEX" => {
                        if items.len() < 4 {
                            return Err(Error::msg("ERR wrong number of arguments for 'setex' command"));
                        }
                        let key = match items[1].clone_bytes() {
                            Some(b) => b,
                            None => return Err(Error::msg("Invalid key for SETEX")),
                        };
                        let seconds = match items[2].to_string() {
                            Ok(s) => s.parse::<i64>()?,
                            _ => return Err(Error::msg("Invalid seconds for SETEX")),
                        };
                        let value = match items[3].clone_bytes() {
                            Some(b) => b,
                            None => return Err(Error::msg("Invalid value for SETEX")),
                        };
                        Ok(RedisCommand::SetEx { key, seconds, value })
                    }
                    "RENAME" => {
                        if items.len() < 3 {
                            return Err(Error::msg("ERR wrong number of arguments for 'rename' command"));
                        }
                        let key = match items[1].clone_bytes() {
                            Some(b) => b,
                            None => return Err(Error::msg("Invalid key for RENAME")),
                        };
                        let newkey = match items[2].clone_bytes() {
                            Some(b) => b,
                            None => return Err(Error::msg("Invalid newkey for RENAME")),
                        };
                        Ok(RedisCommand::Rename { key, newkey })
                    }
                    "HSET" => {
                        if items.len() < 4 || (items.len() - 2) % 2 != 0 {
                            return Err(Error::msg("ERR wrong number of arguments for 'hset' command"));
                        }
                        let key = match items[1].clone_bytes() {
                            Some(b) => b,
                            None => return Err(Error::msg("Invalid key for HSET")),
                        };
                        let mut fields = Vec::new();
                        let mut i = 2;
                        while i < items.len() {
                            let field = match items[i].clone_bytes() {
                                Some(b) => b,
                                None => return Err(Error::msg("Invalid field for HSET")),
                            };
                            let value = match items[i + 1].clone_bytes() {
                                Some(b) => b,
                                None => return Err(Error::msg("Invalid value for HSET")),
                            };
                            fields.push((field, value));
                            i += 2;
                        }
                        Ok(RedisCommand::HSet { key, fields })
                    }
                    "HGET" => {
                        if items.len() < 3 {
                            return Err(Error::msg("ERR wrong number of arguments for 'hget' command"));
                        }
                        let key = match items[1].clone_bytes() {
                            Some(b) => b,
                            None => return Err(Error::msg("Invalid key for HGET")),
                        };
                        let field = match items[2].clone_bytes() {
                            Some(b) => b,
                            None => return Err(Error::msg("Invalid field for HGET")),
                        };
                        Ok(RedisCommand::HGet { key, field })
                    }
                    "HMGET" => {
                        if items.len() < 3 {
                            return Err(Error::msg("ERR wrong number of arguments for 'hmget' command"));
                        }
                        let key = match items[1].clone_bytes() {
                            Some(b) => b,
                            None => return Err(Error::msg("Invalid key for HMGET")),
                        };
                        let mut fields = Vec::new();
                        for i in 2..items.len() {
                            match items[i].clone_bytes() {
                                Some(b) => fields.push(b),
                                None => return Err(Error::msg("Invalid field for HMGET")),
                            }
                        }
                        Ok(RedisCommand::HMGet { key, fields })
                    }
                    "HGETALL" => {
                        if items.len() < 2 {
                            return Err(Error::msg("ERR wrong number of arguments for 'hgetall' command"));
                        }
                        let key = match items[1].clone_bytes() {
                            Some(b) => b,
                            None => return Err(Error::msg("Invalid key for HGETALL")),
                        };
                        Ok(RedisCommand::HGetAll { key })
                    }
                    "HDEL" => {
                        if items.len() < 3 {
                            return Err(Error::msg("ERR wrong number of arguments for 'hdel' command"));
                        }
                        let key = match items[1].clone_bytes() {
                            Some(b) => b,
                            None => return Err(Error::msg("Invalid key for HDEL")),
                        };
                        let mut fields = Vec::new();
                        for i in 2..items.len() {
                            match items[i].clone_bytes() {
                                Some(b) => fields.push(b),
                                None => return Err(Error::msg("Invalid field for HDEL")),
                            }
                        }
                        Ok(RedisCommand::HDel { key, fields })
                    }
                    "HEXISTS" => {
                        if items.len() < 3 {
                            return Err(Error::msg("ERR wrong number of arguments for 'hexists' command"));
                        }
                        let key = match items[1].clone_bytes() {
                            Some(b) => b,
                            None => return Err(Error::msg("Invalid key for HEXISTS")),
                        };
                        let field = match items[2].clone_bytes() {
                            Some(b) => b,
                            None => return Err(Error::msg("Invalid field for HEXISTS")),
                        };
                        Ok(RedisCommand::HExists { key, field })
                    }
                    "HKEYS" => {
                        if items.len() < 2 {
                            return Err(Error::msg("ERR wrong number of arguments for 'hkeys' command"));
                        }
                        let key = match items[1].clone_bytes() {
                            Some(b) => b,
                            None => return Err(Error::msg("Invalid key for HKEYS")),
                        };
                        Ok(RedisCommand::HKeys { key })
                    }
                    "HVALS" => {
                        if items.len() < 2 {
                            return Err(Error::msg("ERR wrong number of arguments for 'hvals' command"));
                        }
                        let key = match items[1].clone_bytes() {
                            Some(b) => b,
                            None => return Err(Error::msg("Invalid key for HVALS")),
                        };
                        Ok(RedisCommand::HVals { key })
                    }
                    "HLEN" => {
                        if items.len() < 2 {
                            return Err(Error::msg("ERR wrong number of arguments for 'hlen' command"));
                        }
                        let key = match items[1].clone_bytes() {
                            Some(b) => b,
                            None => return Err(Error::msg("Invalid key for HLEN")),
                        };
                        Ok(RedisCommand::HLen { key })
                    }
                    "HINCRBY" => {
                        if items.len() < 4 {
                            return Err(Error::msg("ERR wrong number of arguments for 'hincrby' command"));
                        }
                        let key = match items[1].clone_bytes() {
                            Some(b) => b,
                            None => return Err(Error::msg("Invalid key for HINCRBY")),
                        };
                        let field = match items[2].clone_bytes() {
                            Some(b) => b,
                            None => return Err(Error::msg("Invalid field for HINCRBY")),
                        };
                        let increment = match items[3].to_string() {
                            Ok(s) => s.parse::<i64>()?,
                            _ => return Err(Error::msg("Invalid increment for HINCRBY")),
                        };
                        Ok(RedisCommand::HIncrBy { key, field, increment })
                    }
                    "HSETNX" => {
                        if items.len() < 4 {
                            return Err(Error::msg("ERR wrong number of arguments for 'hsetnx' command"));
                        }
                        let key = match items[1].clone_bytes() {
                            Some(b) => b,
                            None => return Err(Error::msg("Invalid key for HSETNX")),
                        };
                        let field = match items[2].clone_bytes() {
                            Some(b) => b,
                            None => return Err(Error::msg("Invalid field for HSETNX")),
                        };
                        let value = match items[3].clone_bytes() {
                            Some(b) => b,
                            None => return Err(Error::msg("Invalid value for HSETNX")),
                        };
                        Ok(RedisCommand::HSetNx { key, field, value })
                    }
                    "SADD" => {
                        if items.len() < 3 {
                            return Err(Error::msg("ERR wrong number of arguments for 'sadd' command"));
                        }
                        let key = match items[1].clone_bytes() {
                            Some(b) => b,
                            None => return Err(Error::msg("Invalid key for SADD")),
                        };
                        let mut members = Vec::with_capacity(items.len() - 2);
                        for i in 2..items.len() {
                            match items[i].clone_bytes() {
                                Some(b) => members.push(b),
                                None => return Err(Error::msg("Invalid member for SADD")),
                            }
                        }
                        Ok(RedisCommand::SAdd { key, members })
                    }
                    "SREM" => {
                        if items.len() < 3 {
                            return Err(Error::msg("ERR wrong number of arguments for 'srem' command"));
                        }
                        let key = match items[1].clone_bytes() {
                            Some(b) => b,
                            None => return Err(Error::msg("Invalid key for SREM")),
                        };
                        let mut members = Vec::with_capacity(items.len() - 2);
                        for i in 2..items.len() {
                            match items[i].clone_bytes() {
                                Some(b) => members.push(b),
                                None => return Err(Error::msg("Invalid member for SREM")),
                            }
                        }
                        Ok(RedisCommand::SRem { key, members })
                    }
                    "SMEMBERS" => {
                        if items.len() < 2 {
                            return Err(Error::msg("ERR wrong number of arguments for 'smembers' command"));
                        }
                        let key = match items[1].clone_bytes() {
                            Some(b) => b,
                            None => return Err(Error::msg("Invalid key for SMEMBERS")),
                        };
                        Ok(RedisCommand::SMembers { key })
                    }
                    "SISMEMBER" => {
                        if items.len() < 3 {
                            return Err(Error::msg("ERR wrong number of arguments for 'sismember' command"));
                        }
                        let key = match items[1].clone_bytes() {
                            Some(b) => b,
                            None => return Err(Error::msg("Invalid key for SISMEMBER")),
                        };
                        let member = match items[2].clone_bytes() {
                            Some(b) => b,
                            None => return Err(Error::msg("Invalid member for SISMEMBER")),
                        };
                        Ok(RedisCommand::SIsMember { key, member })
                    }
                    "SCARD" => {
                        if items.len() < 2 {
                            return Err(Error::msg("ERR wrong number of arguments for 'scard' command"));
                        }
                        let key = match items[1].clone_bytes() {
                            Some(b) => b,
                            None => return Err(Error::msg("Invalid key for SCARD")),
                        };
                        Ok(RedisCommand::SCard { key })
                    }
                    "SPOP" => {
                        if items.len() < 2 {
                            return Err(Error::msg("ERR wrong number of arguments for 'spop' command"));
                        }
                        let key = match items[1].clone_bytes() {
                            Some(b) => b,
                            None => return Err(Error::msg("Invalid key for SPOP")),
                        };
                        let count = if items.len() > 2 {
                            match items[2].to_string() {
                                Ok(s) => match s.parse::<usize>() {
                                    Ok(c) => Some(c),
                                    Err(_) => return Err(Error::msg("ERR value is not an integer or out of range")),
                                },
                                Err(_) => return Err(Error::msg("Invalid count for SPOP")),
                            }
                        } else {
                            None
                        };
                        Ok(RedisCommand::SPop { key, count })
                    }
                    "SRANDMEMBER" => {
                        if items.len() < 2 {
                            return Err(Error::msg("ERR wrong number of arguments for 'srandmember' command"));
                        }
                        let key = match items[1].clone_bytes() {
                            Some(b) => b,
                            None => return Err(Error::msg("Invalid key for SRANDMEMBER")),
                        };
                        let count = if items.len() > 2 {
                            match items[2].to_string() {
                                Ok(s) => match s.parse::<i64>() {
                                    Ok(c) => Some(c),
                                    Err(_) => return Err(Error::msg("ERR value is not an integer or out of range")),
                                },
                                Err(_) => return Err(Error::msg("Invalid count for SRANDMEMBER")),
                            }
                        } else {
                            None
                        };
                        Ok(RedisCommand::SRandMember { key, count })
                    }
                    "MGET" => {
                        if items.len() < 2 {
                            return Err(Error::msg("ERR wrong number of arguments for 'mget' command"));
                        }
                        let mut keys = Vec::with_capacity(items.len() - 1);
                        for item in items.iter().skip(1) {
                            match item.clone_bytes() {
                                Some(b) => keys.push(b),
                                None => return Err(Error::msg("Invalid key for MGET")),
                            }
                        }
                        Ok(RedisCommand::MGet { keys })
                    }
                    "MSET" => {
                        // MSET key value [key value ...]
                        // Must have at least 3 items (MSET + key + value)
                        // And an odd number of arguments after MSET
                        if items.len() < 3 || (items.len() - 1) % 2 != 0 {
                            return Err(Error::msg("ERR wrong number of arguments for 'mset' command"));
                        }
                        let mut pairs = Vec::with_capacity((items.len() - 1) / 2);
                        let mut i = 1;
                        while i < items.len() {
                            let key = match items[i].clone_bytes() {
                                Some(b) => b,
                                None => return Err(Error::msg("Invalid key for MSET")),
                            };
                            let value = match items[i + 1].clone_bytes() {
                                Some(b) => b,
                                None => return Err(Error::msg("Invalid value for MSET")),
                            };
                            pairs.push((key, value));
                            i += 2;
                        }
                        Ok(RedisCommand::MSet { pairs })
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
            RedisCommand::Del { .. } => "del",
            RedisCommand::Exists { .. } => "exists",
            RedisCommand::Expire { .. } => "expire",
            RedisCommand::PExpire { .. } => "pexpire",
            RedisCommand::Ttl { .. } => "ttl",
            RedisCommand::PTtl { .. } => "pttl",
            RedisCommand::Persist { .. } => "persist",
            RedisCommand::Decr { .. } => "decr",
            RedisCommand::DecrBy { .. } => "decrby",
            RedisCommand::IncrBy { .. } => "incrby",
            RedisCommand::Append { .. } => "append",
            RedisCommand::StrLen { .. } => "strlen",
            RedisCommand::SetNx { .. } => "setnx",
            RedisCommand::SetEx { .. } => "setex",
            RedisCommand::Rename { .. } => "rename",
            RedisCommand::HSet { .. } => "hset",
            RedisCommand::HGet { .. } => "hget",
            RedisCommand::HMGet { .. } => "hmget",
            RedisCommand::HGetAll { .. } => "hgetall",
            RedisCommand::HDel { .. } => "hdel",
            RedisCommand::HExists { .. } => "hexists",
            RedisCommand::HKeys { .. } => "hkeys",
            RedisCommand::HVals { .. } => "hvals",
            RedisCommand::HLen { .. } => "hlen",
            RedisCommand::HIncrBy { .. } => "hincrby",
            RedisCommand::HSetNx { .. } => "hsetnx",
            RedisCommand::SAdd { .. } => "sadd",
            RedisCommand::SRem { .. } => "srem",
            RedisCommand::SMembers { .. } => "smembers",
            RedisCommand::SIsMember { .. } => "sismember",
            RedisCommand::SCard { .. } => "scard",
            RedisCommand::SPop { .. } => "spop",
            RedisCommand::SRandMember { .. } => "srandmember",
            RedisCommand::MGet { .. } => "mget",
            RedisCommand::MSet { .. } => "mset",
            RedisCommand::Error { .. } => "error",
            RedisCommand::None => "none",
        }
    }
}
