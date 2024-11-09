use std::collections::HashMap;

#[derive(Debug)]
pub enum RedisCommand<'a> {
    None,
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
}

impl RedisCommand<'_> {
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

    fn validate_entry_id(id: &str, is_xadd: bool) -> Result<(Option<u64>, Option<u64>), String> {
        // Special cases for XRANGE parameters
        if id == "*" || id == "-" || id == "+" {
            return Ok((None, None));
        }

        let parts: Vec<&str> = id.split('-').collect();
        if parts.len() != 2 {
            return Err("ERR Invalid stream ID format".to_string());
        }

        let milliseconds = parts[0].parse::<u64>()
            .map_err(|_| "ERR Invalid milliseconds in stream ID".to_string())?;

        let sequence = if parts[1] == "*" {
            None
        } else {
            Some(parts[1].parse::<u64>()
                .map_err(|_| "ERR Invalid sequence number in stream ID".to_string())?)
        };

        // Only validate 0-0 for XADD
        if is_xadd {
            if let Some(seq) = sequence {
                if milliseconds == 0 && seq == 0 {
                    return Err("ERR The ID specified in XADD must be greater than 0-0".to_string());
                }
            }
        }

        Ok((Some(milliseconds), sequence))
    }

    /// Create command from the data received from the client.
    /// It should check if the parameters are complete, otherwise return None.
    /// For example Set requires 2 parameters, key and value. When this method is called for
    /// a Set command, the first time it will return true to indicate that it expects another.
    pub fn data<'a>(command: &'a str, params: [&'a str;5], original_resp: &'a str) -> Option<RedisCommand<'a>> {
        match command {
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
                    match Self::validate_entry_id(id, true) {
                        Ok(_) => {
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
                        },
                        Err(e) => Some(RedisCommand::Error { message: e }),
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
                    match (Self::validate_entry_id(start, false), Self::validate_entry_id(end, false)) {
                        (Ok(_), Ok(_)) => Some(RedisCommand::XRange { key, start, end }),
                        (Err(e), _) | (_, Err(e)) => Some(RedisCommand::Error { message: e }),
                    }
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

                // Validate all IDs
                for id in &ids {
                    if let Err(e) = Self::validate_entry_id(id, false) {
                        return Some(RedisCommand::Error { message: e });
                    }
                }

                Some(RedisCommand::XRead { keys, ids, block })
            },
            _ => Some(RedisCommand::Error { message: format!("Unknown command: {}", command) }),
        }
    }
}
