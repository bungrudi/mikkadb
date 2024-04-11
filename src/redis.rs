use std::collections::HashMap;
use std::sync::Mutex;

pub enum RedisCommand<'a> {
    Echo { data: &'a str },
    Ping,
    Set { key: &'a str, value: &'a str },
    Get { key: &'a str },
    Error { message: String },
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