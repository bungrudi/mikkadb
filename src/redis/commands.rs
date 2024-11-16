use std::collections::HashMap;

#[derive(Debug)]
pub enum RedisCommand<'a> {
    None,
    Multi,
    Exec,
    Discard,
    Echo { data: &'a str },
    Ping,
    Set { key: &'a str, value: &'a str, ttl: Option<usize>, original_resp: String },
    Get { key: &'a str },
    Info { subcommand: String },
    Replconf { subcommand: &'a str, params: Vec<&'a str> },
    ReplconfGetack,
    Psync { replica_id: &'a str, offset: i8 },
    Wait { numreplicas: i64, timeout: i64, elapsed: i64 },
    Config { subcommand: &'a str, parameter: &'a str },
    Error { message: String },
    Keys { pattern: String },
    Type { key: &'a str },
    XAdd { key: &'a str, id: &'a str, fields: HashMap<String, String>, original_resp: String },
    XRange { key: &'a str, start: &'a str, end: &'a str },
    XRead { keys: Vec<&'a str>, ids: Vec<&'a str>, block: Option<u64> },
    Incr { key: &'a str },
    // List commands
    LPush { key: &'a str, value: &'a str },
    RPush { key: &'a str, value: &'a str },
    LPop { key: &'a str },
    RPop { key: &'a str },
    LLen { key: &'a str },
    LRange { key: &'a str, start: i64, stop: i64 },
    LTrim { key: &'a str, start: i64, stop: i64 },
    LPos { key: &'a str, element: &'a str, count: Option<i64> },
    LInsert { key: &'a str, before: bool, pivot: &'a str, element: &'a str },
    LSet { key: &'a str, index: i64, element: &'a str },
    LIndex { key: &'a str, index: i64 },
}

impl RedisCommand<'_> {
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
    pub fn data<'a>(command: &'a str, params: [&'a str;5], original_resp: &'a str) -> Option<RedisCommand<'a>> {
        match command {
            command if command.eq_ignore_ascii_case(Self::MULTI) => Some(RedisCommand::Multi),
            command if command.eq_ignore_ascii_case(Self::EXEC) => Some(RedisCommand::Exec),
            command if command.eq_ignore_ascii_case(Self::DISCARD) => Some(RedisCommand::Discard),
            command if command.eq_ignore_ascii_case(Self::PING) => Some(RedisCommand::Ping),
            command if command.eq_ignore_ascii_case(Self::ECHO) => {
                if params[0] == "" {
                    None
                } else {
                    Some(RedisCommand::Echo { data: params[0] })
                }
            },
            command if command.eq_ignore_ascii_case(Self::SET) => {
                let key = params[0];
                let value = params[1];
                if key == "" || value == "" {
                    None
                } else {
                    let ttl = match params[2].eq_ignore_ascii_case("EX") {
                        true => match params[3].parse::<usize>() {
                            Ok(value) => Some(value * 1000),
                            Err(_) => None,
                        },
                        false => match params[2].eq_ignore_ascii_case("PX") {
                            true => match params[3].parse::<usize>() {
                                Ok(value) => Some(value),
                                Err(_) => None,
                            },
                            false => None,
                        },
                    };
                    Some(RedisCommand::Set { key, value, ttl, original_resp: original_resp.to_string() })
                }
            },
            command if command.eq_ignore_ascii_case(Self::GET) => {
                let key = params[0];
                if key == "" {
                    None
                } else {
                    Some(RedisCommand::Get { key })
                }
            },
            command if command.eq_ignore_ascii_case(Self::INFO) => {
                if params[0].is_empty() {
                    None
                } else {
                    Some(RedisCommand::Info { subcommand: params[0].to_string() })
                }
            },
            command if command.eq_ignore_ascii_case(Self::REPLCONF) => {
                let subcommand = params[0];
                let params = params[1..].iter().filter(|&&p| !p.is_empty()).cloned().collect();
                if subcommand == "" {
                    Some(RedisCommand::Error { message: "ERR Wrong number of arguments for 'replconf' command".to_string() })
                } else {
                    if subcommand.eq_ignore_ascii_case("getack") {
                        Some(RedisCommand::ReplconfGetack)
                    } else {
                        Some(RedisCommand::Replconf { subcommand, params })
                    }
                }
            },
            command if command.eq_ignore_ascii_case(Self::PSYNC) => {
                let replica_id = params[0];
                match params[1].parse::<i8>() {
                    Ok(offset) => Some(RedisCommand::Psync { replica_id, offset }),
                    Err(_) => Some(RedisCommand::Error { message: "ERR Invalid offset".to_string() }),
                }
            },
            command if command.eq_ignore_ascii_case(Self::WAIT) => {
                let numreplicas = params[0].parse::<i64>().unwrap();
                let timeout = params[1].parse::<i64>().unwrap();
                Some(RedisCommand::Wait { numreplicas, timeout, elapsed: 0 })
            },
            command if command.eq_ignore_ascii_case(Self::CONFIG) => {
                let subcommand = params[0];
                let parameter = params[1];
                if subcommand == "" || parameter == "" {
                    None
                } else {
                    Some(RedisCommand::Config { subcommand, parameter })
                }
            },
            command if command.eq_ignore_ascii_case(Self::KEYS) => {
                if params[0].is_empty() {
                    None
                } else {
                    Some(RedisCommand::Keys { pattern: params[0].to_string() })
                }
            },
            command if command.eq_ignore_ascii_case(Self::TYPE) => {
                let key = params[0];
                if key == "" {
                    None
                } else {
                    Some(RedisCommand::Type { key })
                }
            },
            command if command.eq_ignore_ascii_case(Self::XADD) => {
                let key = params[0];
                let id = params[1];
                if key == "" || id == "" {
                    None
                } else {
                    let mut fields = HashMap::new();
                    let mut i = 2;
                    while i < params.len() - 1 && params[i] != "" && params[i+1] != "" {
                        fields.insert(params[i].to_string(), params[i+1].to_string());
                        i += 2;
                    }
                    if fields.is_empty() {
                        None
                    } else {
                        Some(RedisCommand::XAdd { key, id, fields, original_resp: original_resp.to_string() })
                    }
                }
            },
            command if command.eq_ignore_ascii_case(Self::XRANGE) => {
                let key = params[0];
                let start = params[1];
                let end = params[2];
                if key == "" || start == "" || end == "" {
                    None
                } else {
                    Some(RedisCommand::XRange { key, start, end })
                }
            },
            command if command.eq_ignore_ascii_case(Self::XREAD) => {
                let mut all_params: Vec<&str> = params.iter().copied().filter(|&p| !p.is_empty()).collect();
                let mut block = None;

                // Check for BLOCK option
                if all_params.len() >= 2 && all_params[0].eq_ignore_ascii_case("BLOCK") {
                    if let Ok(timeout) = all_params[1].parse::<u64>() {
                        block = Some(timeout);
                        all_params.drain(0..2); // Remove BLOCK and timeout
                    }
                }

                // Check for STREAMS keyword
                if all_params.is_empty() || !all_params[0].eq_ignore_ascii_case("STREAMS") {
                    return Some(RedisCommand::Error { message: "ERR wrong number of arguments for 'xread' command".to_string() });
                }
                all_params.remove(0); // Remove STREAMS

                if all_params.is_empty() {
                    return Some(RedisCommand::Error { message: "ERR wrong number of arguments for 'xread' command".to_string() });
                }

                let midpoint = all_params.len() / 2;
                if midpoint == 0 || all_params.len() % 2 != 0 {
                    return Some(RedisCommand::Error { message: "ERR wrong number of arguments for 'xread' command".to_string() });
                }

                // Split into keys and ids
                let keys = all_params[..midpoint].to_vec();
                let ids = all_params[midpoint..].to_vec();

                Some(RedisCommand::XRead { keys, ids, block })
            },
            command if command.eq_ignore_ascii_case(Self::INCR) => {
                let key = params[0];
                if key == "" {
                    None
                } else {
                    Some(RedisCommand::Incr { key })
                }
            },
            // List commands
            command if command.eq_ignore_ascii_case(Self::LPUSH) => {
                let key = params[0];
                let value = params[1];
                if key == "" || value == "" {
                    None
                } else {
                    Some(RedisCommand::LPush { key, value })
                }
            },
            command if command.eq_ignore_ascii_case(Self::RPUSH) => {
                let key = params[0];
                let value = params[1];
                if key == "" || value == "" {
                    None
                } else {
                    Some(RedisCommand::RPush { key, value })
                }
            },
            command if command.eq_ignore_ascii_case(Self::LPOP) => {
                let key = params[0];
                if key == "" {
                    None
                } else {
                    Some(RedisCommand::LPop { key })
                }
            },
            command if command.eq_ignore_ascii_case(Self::RPOP) => {
                let key = params[0];
                if key == "" {
                    None
                } else {
                    Some(RedisCommand::RPop { key })
                }
            },
            command if command.eq_ignore_ascii_case(Self::LLEN) => {
                let key = params[0];
                if key == "" {
                    None
                } else {
                    Some(RedisCommand::LLen { key })
                }
            },
            command if command.eq_ignore_ascii_case(Self::LRANGE) => {
                let key = params[0];
                let start = params[1].parse::<i64>().unwrap_or(0);
                let stop = params[2].parse::<i64>().unwrap_or(-1);
                if key == "" {
                    None
                } else {
                    Some(RedisCommand::LRange { key, start, stop })
                }
            },
            command if command.eq_ignore_ascii_case(Self::LTRIM) => {
                let key = params[0];
                let start = params[1].parse::<i64>().unwrap_or(0);
                let stop = params[2].parse::<i64>().unwrap_or(-1);
                if key == "" {
                    None
                } else {
                    Some(RedisCommand::LTrim { key, start, stop })
                }
            },
            command if command.eq_ignore_ascii_case(Self::LPOS) => {
                let key = params[0];
                let element = params[1];
                let count = if params[2].eq_ignore_ascii_case("COUNT") {
                    params[3].parse::<i64>().ok()
                } else {
                    None
                };
                if key == "" || element == "" {
                    None
                } else {
                    Some(RedisCommand::LPos { key, element, count })
                }
            },
            command if command.eq_ignore_ascii_case(Self::LINSERT) => {
                let key = params[0];
                let before = params[1].eq_ignore_ascii_case("BEFORE");
                let pivot = params[2];
                let element = params[3];
                if key == "" || pivot == "" || element == "" {
                    None
                } else {
                    Some(RedisCommand::LInsert { key, before, pivot, element })
                }
            },
            command if command.eq_ignore_ascii_case(Self::LSET) => {
                let key = params[0];
                let index = params[1].parse::<i64>().unwrap_or(0);
                let element = params[2];
                if key == "" || element == "" {
                    None
                } else {
                    Some(RedisCommand::LSet { key, index, element })
                }
            },
            command if command.eq_ignore_ascii_case(Self::LINDEX) => {
                let key = params[0];
                let index = params[1].parse::<i64>().unwrap_or(0);
                if key == "" {
                    None
                } else {
                    Some(RedisCommand::LIndex { key, index })
                }
            },
            _ => Some(RedisCommand::Error { message: format!("Unknown command: {}", command) }),
        }
    }
}
