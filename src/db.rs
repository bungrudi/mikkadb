use std::collections::HashMap;
use bytes::Bytes;
use std::time::{Instant, Duration};

#[derive(Clone)]
pub struct Db {
    data: HashMap<String, (Bytes, Option<Instant>)>,
}

impl Db {
    pub fn new() -> Self {
        Db {
            data: HashMap::new(),
        }
    }

    pub fn set(&mut self, key: String, value: Bytes, px: Option<u64>) {
        let expiry = px.map(|ms| Instant::now() + Duration::from_millis(ms));
        self.data.insert(key, (value, expiry));
    }

    pub fn get(&mut self, key: &str) -> Option<Bytes> {
        if let Some((value, expiry)) = self.data.get(key) {
            if let Some(expiry_time) = expiry {
                if Instant::now() > *expiry_time {
                    self.data.remove(key);
                    return None;
                }
            }
            return Some(value.clone());
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
