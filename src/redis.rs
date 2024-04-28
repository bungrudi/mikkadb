use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug)]
pub enum RedisCommand<'a> {
    None,
    Echo { data: &'a str },
    Ping,
    Set { key: &'a str, value: &'a str, ttl: Option<usize> },
    Get { key: &'a str },
    Error { message: String },
}

impl RedisCommand<'_> {
    const PING : &'static str = "PING";
    const ECHO : &'static str = "ECHO";
    const SET : &'static str = "SET";
    const GET : &'static str = "GET";

    pub fn is_none(&self) -> bool {
        match self {
            RedisCommand::None => true,
            _ => false,
        }
    }

    /// Create command from the data received from the client.
    /// It should check if the parameters are complete, otherwise return None.
    /// For example Set requires 2 parameters, key and value. When this method is called for
    /// a Set command, the first time it will return true to indicate that it expects another.
    pub fn data<'a>(command: &'a str, params: [&'a str;5]) -> Option<RedisCommand<'a>> {
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
                    Some(RedisCommand::Set { key, value, ttl })
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
            _ => Some(RedisCommand::Error { message: format!("Unknown command: {}", command) }),
        }
    }
}

struct ValueWrapper {
    value: String,
    expiration: Option<u128>,
}

pub struct Redis {
    data: Mutex<HashMap<String, ValueWrapper>>,
}
impl Redis {
    pub fn new() -> Self {
        Redis {
            data: Mutex::new(HashMap::new()),
        }
    }

    pub fn set(&mut self, key: &str, value: &str, ttl: Option<usize>) {
        let expiration =
            match ttl {
                Some(ttl) => {
                    if ttl == 0 {
                        None
                    } else {
                        Some(SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_millis() + ttl as u128)
                    }
                },
                None => None,
            };
        let value_with_ttl = ValueWrapper { value: value.to_string(), expiration: expiration };
        self.data.lock().unwrap().insert(key.to_string(), value_with_ttl);
    }

    pub fn get(&self, key: &str) -> Option<String> {
        let mut binding = self.data.lock().unwrap();
        let value_wrapper = binding.get(key);
        match value_wrapper {
            Some(value_with_ttl) => {
                match value_with_ttl.expiration {
                    Some(expiration) => {
                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_millis();
                        if now > expiration {
                            binding.remove(key);
                            None
                        } else {
                            Some(value_with_ttl.value.clone())
                        }
                    },
                    None => Some(value_with_ttl.value.clone()),
                }
            },
            None => None,
        }
    }

    pub fn execute_command(&mut self, command: &RedisCommand) -> Result<String, String> {
        match command {
            RedisCommand::Ping => {
                Ok("+PONG".to_string())
            },
            RedisCommand::Echo { data } => {
                Ok(format!("${}\r\n{}", data.len(), data))
            },
            RedisCommand::Get { key } => {
                match self.get(key) {
                    Some(value) => Ok(format!("${}\r\n{}", value.len(), value)),
                    None => Ok("$-1".to_string()),
                }
            },
            RedisCommand::Set { key, value, ttl } => {
                self.set(key, value, *ttl);
                Ok("+OK".to_string())
            },
            RedisCommand::Error { message } => {
                Err(format!("ERR {}", message))
            },
            _ => Err("ERR Unknown command".to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let db = Redis::new();
        assert!(db.data.lock().unwrap().is_empty());
    }

    #[test]
    fn test_set() {
        let mut db = Redis::new();
        db.set("key1", "value1", None);
        assert_eq!(db.get("key1"), Some("value1".to_string()));
    }

    #[test]
    fn test_get() {
        let mut db = Redis::new();
        db.set("key1", "value1", None);
        assert_eq!(db.get("key1"), Some("value1".to_string()));
    }

    #[test]
    fn test_set_ttl_and_get() {
        let mut db = Redis::new();
        db.set("key1", "value1", Some(1000));
        assert_eq!(db.get("key1"), Some("value1".to_string()));
        // sleep for 1 second
        std::thread::sleep(std::time::Duration::from_secs(1));
        assert_eq!(db.get("key1"), None);
    }

    #[test]
    fn test_get_nonexistent() {
        let db = Redis::new();
        assert_eq!(db.get("key1"), None);
    }

    /// Test create RedisCommand::Set with TTL in milliseconds.
    /// SET key value PX 1000
    #[test]
    fn test_set_ttl_px() {
        let command = RedisCommand::data("SET", ["key", "value", "PX", "1000", ""]);
        match command {
            Some(RedisCommand::Set { key, value, ttl }) => {
                assert_eq!(key, "key");
                assert_eq!(value, "value");
                assert_eq!(ttl, Some(1000));
            },
            _ => panic!("Expected RedisCommand::Set"),
        }

    }

    /// Test create RedisCommand::Set with TTL in seconds.
    /// SET key value EX 1
    #[test]
    fn test_set_ttl_ex() {
        let command = RedisCommand::data("SET", ["key", "value", "EX", "1", ""]);
        match command {
            Some(RedisCommand::Set { key, value, ttl }) => {
                assert_eq!(key, "key");
                assert_eq!(value, "value");
                assert_eq!(ttl, Some(1000));
            },
            _ => panic!("Expected RedisCommand::Set"),
        }
    }
}