use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use bytes::Bytes;

#[derive(Clone)]
pub struct Db {
    storage: Arc<Mutex<HashMap<String, Bytes>>>,
}

impl Db {
    pub fn new() -> Self {
        Db {
            storage: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn set(&self, key: String, value: Bytes) {
        let mut storage = self.storage.lock().unwrap();
        storage.insert(key, value);
    }

    pub fn get(&self, key: &str) -> Option<Bytes> {
        let storage = self.storage.lock().unwrap();
        storage.get(key).cloned()
    }
}
