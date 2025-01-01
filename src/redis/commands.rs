use std::collections::HashMap;
use super::xread_parser;

#[derive(Debug, Clone)]
pub enum RedisCommand {
    /// Represents an invalid or unrecognized command.
    /// This variant is used in pattern matching but never constructed directly.
    #[allow(dead_code)]
    None,
    Multi,
    Exec,
    Discard,
    Echo { data: String },
    Ping,
    Set { key: String, value: String, ttl: Option<usize>, original_resp: String }, 
    Get { key: String },
    Info { subcommand: String },
    Replconf { subcommand: String, params: Vec<String> },
    ReplconfGetack,
    Psync { replica_id: String, offset: i8 },
    Wait { numreplicas: i64, timeout: i64, elapsed: i64 },
    Config { subcommand: String, parameter: String },
    Error { message: String },
    Keys { pattern: String },
    Type { key: String },
    XAdd { key: String, id: String, fields: HashMap<String, String>, original_resp: String },
    XRange { key: String, start: String, end: String },
    XRead { keys: Vec<String>, ids: Vec<String>, block: Option<u64>, count: Option<usize> },
    Incr { key: String },
    FlushDB,
    // List commands
    LPush { key: String, value: String },
    RPush { key: String, value: String },
    LPop { key: String },
    RPop { key: String },
    LLen { key: String },
    LRange { key: String, start: i64, stop: i64 },
    LTrim { key: String, start: i64, stop: i64 },
    LPos { key: String, element: String, count: Option<i64> },
    LInsert { key: String, before: bool, pivot: String, element: String },
    LSet { key: String, index: i64, element: String },
    LIndex { key: String, index: i64 },
}

impl RedisCommand {
    const MULTI : &'static str = "MULTI";
    const EXEC : &'static str = "EXEC";
    const DISCARD : &'static str = "DISCARD";
    const PING : &'static str = "PING";
    const ECHO : &'static str = "ECHO";
    const SET : &'static str = "SET";
    const GET : &'static str = "GET";
    const INFO : &'static str = "INFO";
    const REPLCONF : &'static str = "REPLCONF";
    const PSYNC : &'static str = "PSYNC";
    const WAIT: &'static str = "WAIT";
    const CONFIG: &'static str = "CONFIG";
    const KEYS: &'static str = "KEYS";
    const TYPE: &'static str = "TYPE";
    const XADD: &'static str = "XADD";
    const XRANGE: &'static str = "XRANGE";
    const XREAD: &'static str = "XREAD";
    const INCR: &'static str = "INCR";
    const FLUSHDB: &'static str = "FLUSHDB";
    // List command constants
    const LPUSH: &'static str = "LPUSH";
    const RPUSH: &'static str = "RPUSH";
    const LPOP: &'static str = "LPOP";
    const RPOP: &'static str = "RPOP";
    const LLEN: &'static str = "LLEN";
    const LRANGE: &'static str = "LRANGE";
    const LTRIM: &'static str = "LTRIM";
    const LPOS: &'static str = "LPOS";
    const LINSERT: &'static str = "LINSERT";
    const LSET: &'static str = "LSET";
    const LINDEX: &'static str = "LINDEX";

    /// Create command from the data received from the client.
    /// It should check if the parameters are complete, otherwise return None.
    /// For example Set requires 2 parameters, key and value. When this method is called for
    /// a Set command, the first time it will return true to indicate that it expects another.
    pub fn data(command: String, params: &[String], original_resp: String) -> Option<RedisCommand> {
        match command.to_ascii_uppercase().as_str() {
            ref command if command.eq_ignore_ascii_case(Self::MULTI) => Some(RedisCommand::Multi),
            ref command if command.eq_ignore_ascii_case(Self::EXEC) => Some(RedisCommand::Exec),
            ref command if command.eq_ignore_ascii_case(Self::DISCARD) => Some(RedisCommand::Discard),
            ref command if command.eq_ignore_ascii_case(Self::PING) => Some(RedisCommand::Ping),
            ref command if command.eq_ignore_ascii_case(Self::ECHO) => {
                if params.is_empty() {
                    None
                } else {
                    Some(RedisCommand::Echo { data: params[0].clone() })
                }
            },
            ref command if command.eq_ignore_ascii_case(Self::SET) => {
                if params.len() < 2 {
                    None
                } else {
                    let key = &params[0];
                    let value = &params[1];
                    let ttl = match params.get(2) {
                        Some(param) if param.eq_ignore_ascii_case("EX") => match params.get(3) {
                            Some(param) => match param.parse::<usize>() {
                                Ok(value) => Some(value * 1000),
                                Err(_) => None,
                            },
                            None => None,
                        },
                        Some(param) if param.eq_ignore_ascii_case("PX") => match params.get(3) {
                            Some(param) => match param.parse::<usize>() {
                                Ok(value) => Some(value),
                                Err(_) => None,
                            },
                            None => None,
                        },
                        _ => None,
                    };
                    Some(RedisCommand::Set { 
                        key: key.clone(), 
                        value: value.clone(), 
                        ttl, 
                        original_resp 
                    })
                }
            },
            command if command.eq_ignore_ascii_case(Self::GET) => {
                if params.is_empty() {
                    None
                } else {
                    let key = params[0].clone();
                    Some(RedisCommand::Get { key })
                }
            },
            command if command.eq_ignore_ascii_case(Self::INFO) => {
                if params.is_empty() {
                    None
                } else {
                    let _subcommand = params[0].clone();
                    Some(RedisCommand::Info { subcommand: params[0].to_string() })
                }
            },
            command if command.eq_ignore_ascii_case(Self::REPLCONF) => {
                if params.is_empty() {
                    None
                } else {
                    let _subcommand = params[0].clone();
                    let params = params[1..].iter().filter(|&p| !p.is_empty()).cloned().collect();
                    if _subcommand.is_empty() {
                        Some(RedisCommand::Error { message: "ERR Wrong number of arguments for 'replconf' command".to_string() })
                    } else {
                        if _subcommand.eq_ignore_ascii_case("getack") {
                            Some(RedisCommand::ReplconfGetack)
                        } else {
                            Some(RedisCommand::Replconf { subcommand: _subcommand, params })
                        }
                    }
                }
            },
            command if command.eq_ignore_ascii_case(Self::PSYNC) => {
                if params.len() < 2 {
                    None
                } else {
                    let replica_id = params[0].clone();
                    match params[1].parse::<i8>() {
                        Ok(offset) => Some(RedisCommand::Psync { replica_id, offset }),
                        Err(_) => Some(RedisCommand::Error { message: "ERR Invalid offset".to_string() }),
                    }
                }
            },
            command if command.eq_ignore_ascii_case(Self::WAIT) => {
                if params.len() < 2 {
                    None
                } else {
                    let numreplicas = params[0].parse::<i64>().unwrap();
                    let timeout = params[1].parse::<i64>().unwrap();
                    Some(RedisCommand::Wait { numreplicas, timeout, elapsed: 0 })
                }
            },
            command if command.eq_ignore_ascii_case(Self::CONFIG) => {
                if params.len() < 2 {
                    None
                } else {
                    let _subcommand = params[0].clone();
                    let parameter = params[1].clone();
                    if _subcommand.is_empty() || parameter.is_empty() {
                        None
                    } else {
                        Some(RedisCommand::Config { subcommand: _subcommand, parameter })
                    }
                }
            },
            command if command.eq_ignore_ascii_case(Self::KEYS) => {
                if params.is_empty() {
                    None
                } else {
                    Some(RedisCommand::Keys { pattern: params[0].clone() })
                }
            },
            command if command.eq_ignore_ascii_case(Self::TYPE) => {
                if params.is_empty() {
                    None
                } else {
                    let key = params[0].clone();
                    Some(RedisCommand::Type { key })
                }
            },
            command if command.eq_ignore_ascii_case(Self::XADD) => {
                if params.len() < 3 {
                    None
                } else {
                    let key = params[0].clone();
                    let id = params[1].clone();
                    if key.is_empty() || id.is_empty() {
                        None
                    } else {
                        let mut fields = HashMap::new();
                        let mut i = 2;
                        while i < params.len() - 1 && !params[i].is_empty() && !params[i+1].is_empty() {
                            fields.insert(params[i].clone(), params[i+1].clone());
                            i += 2;
                        }
                        if fields.is_empty() {
                            None
                        } else {
                            Some(RedisCommand::XAdd { key, id, fields, original_resp })
                        }
                    }
                }
            },
            command if command.eq_ignore_ascii_case(Self::XRANGE) => {
                if params.len() < 3 {
                    None
                } else {
                    let key = params[0].clone();
                    let start = params[1].clone();
                    let end = params[2].clone();
                    if key.is_empty() || start.is_empty() || end.is_empty() {
                        None
                    } else {
                        Some(RedisCommand::XRange { key, start, end })
                    }
                }
            },
            command if command.eq_ignore_ascii_case(Self::XREAD) => {
                let all_params: Vec<String> = params.iter()
                    .take_while(|p| !p.is_empty())
                    .cloned()
                    .collect();

                match xread_parser::parse_xread(&all_params) {
                    Ok(result) => Some(RedisCommand::XRead {
                        keys: result.keys,
                        ids: result.ids,
                        block: result.block,
                        count: result.count,
                    }),
                    Err(msg) => Some(RedisCommand::Error { message: msg }),
                }
            },
            command if command.eq_ignore_ascii_case(Self::INCR) => {
                if params.is_empty() {
                    None
                } else {
                    let key = params[0].clone();
                    Some(RedisCommand::Incr { key })
                }
            },
            command if command.eq_ignore_ascii_case(Self::FLUSHDB) => {
                Some(RedisCommand::FlushDB)
            },
            // List commands
            command if command.eq_ignore_ascii_case(Self::LPUSH) => {
                if params.len() < 2 {
                    None
                } else {
                    let key = params[0].clone();
                    let value = params[1].clone();
                    if key.is_empty() || value.is_empty() {
                        None
                    } else {
                        Some(RedisCommand::LPush { key, value })
                    }
                }
            },
            command if command.eq_ignore_ascii_case(Self::RPUSH) => {
                if params.len() < 2 {
                    None
                } else {
                    let key = params[0].clone();
                    let value = params[1].clone();
                    if key.is_empty() || value.is_empty() {
                        None
                    } else {
                        Some(RedisCommand::RPush { key, value })
                    }
                }
            },
            command if command.eq_ignore_ascii_case(Self::LPOP) => {
                if params.is_empty() {
                    None
                } else {
                    let key = params[0].clone();
                    Some(RedisCommand::LPop { key })
                }
            },
            command if command.eq_ignore_ascii_case(Self::RPOP) => {
                if params.is_empty() {
                    None
                } else {
                    let key = params[0].clone();
                    Some(RedisCommand::RPop { key })
                }
            },
            command if command.eq_ignore_ascii_case(Self::LLEN) => {
                if params.is_empty() {
                    None
                } else {
                    let key = params[0].clone();
                    Some(RedisCommand::LLen { key })
                }
            },
            command if command.eq_ignore_ascii_case(Self::LRANGE) => {
                if params.len() < 3 {
                    None
                } else {
                    let key = params[0].clone();
                    let start = params[1].parse::<i64>().unwrap_or(0);
                    let stop = params[2].parse::<i64>().unwrap_or(-1);
                    if key.is_empty() {
                        None
                    } else {
                        Some(RedisCommand::LRange { key, start, stop })
                    }
                }
            },
            command if command.eq_ignore_ascii_case(Self::LTRIM) => {
                if params.len() < 3 {
                    None
                } else {
                    let key = params[0].clone();
                    let start = params[1].parse::<i64>().unwrap_or(0);
                    let stop = params[2].parse::<i64>().unwrap_or(-1);
                    if key.is_empty() {
                        None
                    } else {
                        Some(RedisCommand::LTrim { key, start, stop })
                    }
                }
            },
            command if command.eq_ignore_ascii_case(Self::LPOS) => {
                if params.len() < 2 {
                    None
                } else {
                    let key = params[0].clone();
                    let element = params[1].clone();
                    let mut count = None;
                    let mut i = 2;
                    while i < params.len() {
                        if params[i].eq_ignore_ascii_case("COUNT") && i + 1 < params.len() {
                            count = params[i + 1].parse::<i64>().ok();
                            break;
                        }
                        i += 1;
                    }
                    if key.is_empty() || element.is_empty() {
                        None
                    } else {
                        Some(RedisCommand::LPos { key, element, count })
                    }
                }
            },
            command if command.eq_ignore_ascii_case(Self::LINSERT) => {
                if params.len() < 4 {
                    None
                } else {
                    let key = params[0].clone();
                    let before = params[1].eq_ignore_ascii_case("BEFORE");
                    let pivot = params[2].clone();
                    let element = params[3].clone();
                    if key.is_empty() || pivot.is_empty() || element.is_empty() {
                        None
                    } else {
                        Some(RedisCommand::LInsert { key, before, pivot, element })
                    }
                }
            },
            command if command.eq_ignore_ascii_case(Self::LSET) => {
                if params.len() < 3 {
                    None
                } else {
                    let key = params[0].clone();
                    let index = params[1].parse::<i64>().unwrap_or(0);
                    let element = params[2].clone();
                    if key.is_empty() || element.is_empty() {
                        None
                    } else {
                        Some(RedisCommand::LSet { key, index, element })
                    }
                }
            },
            command if command.eq_ignore_ascii_case(Self::LINDEX) => {
                if params.len() < 2 {
                    None
                } else {
                    let key = params[0].clone();
                    let index = params[1].parse::<i64>().unwrap_or(0);
                    if key.is_empty() {
                        None
                    } else {
                        Some(RedisCommand::LIndex { key, index })
                    }
                }
            },
            _ => Some(RedisCommand::Error { message: format!("Unknown command: {}", command) }),
        }
    }
}
