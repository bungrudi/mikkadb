use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use bytes::Bytes;

#[derive(Clone)]
pub struct Db {
    storage: Arc<Mutex<HashMap<String, (Bytes, Option<Instant>)>>>,
}

impl Db {
    pub fn new() -> Self {
        Db {
            storage: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn set(&self, key: String, value: Bytes, px: Option<u64>) {
        let mut storage = self.storage.lock().unwrap();
        let expiry = px.map(|ms| Instant::now() + Duration::from_millis(ms));
        storage.insert(key, (value, expiry));
    }

    pub fn get(&self, key: &str) -> Option<Bytes> {
        let mut storage = self.storage.lock().unwrap();
        if let Some((value, expiry)) = storage.get(key).cloned() {
            if let Some(expiry_time) = expiry {
                if Instant::now() > expiry_time {
                    storage.remove(key);
                    return None;
                }
            }
            return Some(value);
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_set_get() {
        let db = Db::new();
        db.set("key".to_string(), Bytes::from("value"), None);
        assert_eq!(db.get("key"), Some(Bytes::from("value")));
    }

    #[test]
    fn test_expiry() {
        let db = Db::new();
        db.set("key".to_string(), Bytes::from("value"), Some(100));
        assert_eq!(db.get("key"), Some(Bytes::from("value")));
        
        thread::sleep(Duration::from_millis(150));
        assert_eq!(db.get("key"), None);
    }
}
