use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

struct ValueWrapper {
    value: String,
    expiration: Option<u64>,
}

pub struct Storage {
    data: Mutex<HashMap<String, ValueWrapper>>,
}

impl Storage {
    pub fn new() -> Self {
        Storage {
            data: Mutex::new(HashMap::new()),
        }
    }

    pub fn set(&self, key: &str, value: &str, ttl: Option<usize>) {
        let mut data = self.data.lock().unwrap();
        let expiration = ttl.map(|ttl| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64
                + ttl as u64
        });
        println!("DEBUG: Setting key '{}' with value '{}' and expiration {:?}", key, value, expiration);
        data.insert(
            key.to_string(),
            ValueWrapper {
                value: value.to_string(),
                expiration,
            },
        );
    }

    pub fn get(&self, key: &str) -> Option<String> {
        let mut data = self.data.lock().unwrap();
        if let Some(wrapper) = data.get(key) {
            if let Some(expiration) = wrapper.expiration {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;
                if now > expiration {
                    println!("DEBUG: Key '{}' has expired. Current time: {}, Expiration: {}", key, now, expiration);
                    data.remove(key);
                    return None;
                }
            }
            println!("DEBUG: Retrieved key '{}' with value '{}'", key, wrapper.value);
            Some(wrapper.value.clone())
        } else {
            println!("DEBUG: Key '{}' not found", key);
            None
        }
    }

    pub fn keys(&self, pattern: &str) -> Vec<String> {
        let data = self.data.lock().unwrap();
        if pattern == "*" {
            data.keys()
                .filter(|k| !k.starts_with("redis-"))
                .cloned()
                .collect()
        } else {
            vec![]
        }
    }
}
