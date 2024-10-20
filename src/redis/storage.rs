use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug)]
pub struct StreamEntry {
    pub id: String,
    pub fields: HashMap<String, String>,
}

pub enum ValueWrapper {
    String {
        value: String,
        expiration: Option<u64>,
    },
    Stream {
        entries: Vec<StreamEntry>,
    },
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
            ValueWrapper::String {
                value: value.to_string(),
                expiration,
            },
        );
    }

    pub fn get(&self, key: &str) -> Option<String> {
        let mut data = self.data.lock().unwrap();
        if let Some(wrapper) = data.get(key) {
            match wrapper {
                ValueWrapper::String { value, expiration } => {
                    if let Some(expiration) = expiration {
                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_millis() as u64;
                        if now > *expiration {
                            println!("DEBUG: Key '{}' has expired. Current time: {}, Expiration: {}", key, now, expiration);
                            data.remove(key);
                            return None;
                        }
                    }
                    println!("DEBUG: Retrieved key '{}' with value '{}'", key, value);
                    Some(value.clone())
                },
                ValueWrapper::Stream { .. } => None,
            }
        } else {
            println!("DEBUG: Key '{}' not found", key);
            None
        }
    }

    fn compare_stream_ids(id1: &str, id2: &str) -> std::cmp::Ordering {
        let parts1: Vec<&str> = id1.split('-').collect();
        let parts2: Vec<&str> = id2.split('-').collect();

        let ms1 = parts1[0].parse::<u64>().unwrap();
        let ms2 = parts2[0].parse::<u64>().unwrap();

        if ms1 != ms2 {
            ms1.cmp(&ms2)
        } else {
            let seq1 = parts1[1].parse::<u64>().unwrap();
            let seq2 = parts2[1].parse::<u64>().unwrap();
            seq1.cmp(&seq2)
        }
    }

    pub fn xadd(&self, key: &str, id: &str, fields: HashMap<String, String>) -> Result<String, String> {
        let mut data = self.data.lock().unwrap();
        let entry = StreamEntry {
            id: id.to_string(),
            fields,
        };

        match data.entry(key.to_string()) {
            std::collections::hash_map::Entry::Occupied(mut occupied) => {
                match occupied.get_mut() {
                    ValueWrapper::Stream { entries } => {
                        if let Some(last_entry) = entries.last() {
                            if Self::compare_stream_ids(&entry.id, &last_entry.id) != std::cmp::Ordering::Greater {
                                return Err("ERR The ID specified in XADD is equal or smaller than the target stream top item".to_string());
                            }
                        }
                        entries.push(entry);
                    },
                    _ => return Err("ERR WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                }
            },
            std::collections::hash_map::Entry::Vacant(vacant) => {
                if Self::compare_stream_ids(id, "0-0") != std::cmp::Ordering::Greater {
                    return Err("ERR The ID specified in XADD must be greater than 0-0".to_string());
                }
                vacant.insert(ValueWrapper::Stream { entries: vec![entry] });
            },
        }

        Ok(id.to_string())
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

    pub fn get_type(&self, key: &str) -> String {
        let data = self.data.lock().unwrap();
        match data.get(key) {
            Some(ValueWrapper::String { .. }) => "string".to_string(),
            Some(ValueWrapper::Stream { .. }) => "stream".to_string(),
            None => "none".to_string(),
        }
    }
}
