use std::collections::HashMap;
use std::sync::Mutex;

pub enum RedisCommand<'a> {
    Echo { data: &'a str },
    Ping,
    Set { key: &'a str, value: &'a str },
    Get { key: &'a str },
    Error { message: String },
}

impl RedisCommand<'_> {
    const PING : &'static str = "PING";
    const ECHO : &'static str = "ECHO";
    const SET : &'static str = "SET";
    const GET : &'static str = "GET";

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
                    Some(RedisCommand::Set { key, value })
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

pub struct Redis {
    data: Mutex<HashMap<String, String>>,
}
impl Redis {
    pub fn new() -> Self {
        Redis {
            data: Mutex::new(HashMap::new()),
        }
    }

    pub fn set(&mut self, key: &str, value: &str) {
        self.data.lock().unwrap().insert(key.to_string(), value.to_string());
    }

    pub fn get(&self, key: &str) -> Option<String> {
        self.data.lock().unwrap().get(key).cloned()
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
        db.set("key1", "value1");
        assert_eq!(db.get("key1"), Some("value1".to_string()));
    }

    #[test]
    fn test_get() {
        let mut db = Redis::new();
        db.set("key1", "value1");
        assert_eq!(db.get("key1"), Some("value1".to_string()));
    }

    #[test]
    fn test_get_nonexistent() {
        let db = Redis::new();
        assert_eq!(db.get("key1"), None);
    }
}