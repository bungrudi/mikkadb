use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug)]
pub struct StreamEntry {
    pub id: String,
    pub fields: HashMap<String, String>,
}

#[derive(Default)]
pub struct StreamMetadata {
    last_sequences: HashMap<u64, u64>, // Maps time_part to its last sequence
}

pub enum ValueWrapper {
    String {
        value: String,
        expiration: Option<u64>,
    },
    Stream {
        entries: Vec<StreamEntry>,
        metadata: StreamMetadata,
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

    fn get_next_sequence(metadata: &mut StreamMetadata, time_part: u64) -> u64 {
        let next_seq = match metadata.last_sequences.get(&time_part) {
            Some(&seq) => seq + 1,
            None => if time_part == 0 { 1 } else { 0 }
        };
        metadata.last_sequences.insert(time_part, next_seq);
        next_seq
    }

    fn validate_new_id(&self, entries: &[StreamEntry], new_id: &str) -> Result<(), String> {
        if let Some(last_entry) = entries.last() {
            if Self::compare_stream_ids(new_id, &last_entry.id) != std::cmp::Ordering::Greater {
                return Err("ERR The ID specified in XADD is equal or smaller than the target stream top item".to_string());
            }
        }
        Ok(())
    }

    pub fn get_current_time_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    pub fn xadd(&self, key: &str, id: &str, fields: HashMap<String, String>) -> Result<String, String> {
        let mut data = self.data.lock().unwrap();
        
        // Handle auto-generation of ID
        let (time_part, sequence) = if id == "*" {
            (Self::get_current_time_ms(), None)
        } else {
            let parts: Vec<&str> = id.split('-').collect();
            let time = parts[0].parse::<u64>().unwrap();
            let is_auto_seq = parts[1] == "*";
            (time, if is_auto_seq { None } else { Some(parts[1].parse::<u64>().unwrap()) })
        };

        match data.entry(key.to_string()) {
            std::collections::hash_map::Entry::Occupied(mut occupied) => {
                match occupied.get_mut() {
                    ValueWrapper::Stream { entries, metadata } => {
                        let sequence = sequence.unwrap_or_else(|| Self::get_next_sequence(metadata, time_part));
                        let new_id = format!("{}-{}", time_part, sequence);
                        
                        self.validate_new_id(entries, &new_id)?;

                        let entry = StreamEntry {
                            id: new_id.clone(),
                            fields,
                        };
                        entries.push(entry);
                        Ok(new_id)
                    },
                    _ => Err("ERR WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                }
            },
            std::collections::hash_map::Entry::Vacant(vacant) => {
                let mut metadata = StreamMetadata::default();
                let sequence = sequence.unwrap_or_else(|| Self::get_next_sequence(&mut metadata, time_part));
                let new_id = format!("{}-{}", time_part, sequence);

                if Self::compare_stream_ids(&new_id, "0-0") != std::cmp::Ordering::Greater {
                    return Err("ERR The ID specified in XADD must be greater than 0-0".to_string());
                }

                let entry = StreamEntry {
                    id: new_id.clone(),
                    fields,
                };
                vacant.insert(ValueWrapper::Stream { 
                    entries: vec![entry],
                    metadata,
                });
                Ok(new_id)
            },
        }
    }

    pub fn xrange(&self, key: &str, start: &str, end: &str) -> Result<Vec<StreamEntry>, String> {
        let data = self.data.lock().unwrap();
        
        match data.get(key) {
            Some(ValueWrapper::Stream { entries, .. }) => {
                let filtered_entries: Vec<StreamEntry> = entries.iter()
                    .filter(|entry| {
                        Self::compare_stream_ids(&entry.id, start) >= std::cmp::Ordering::Equal &&
                        Self::compare_stream_ids(&entry.id, end) <= std::cmp::Ordering::Equal
                    })
                    .cloned()
                    .collect();
                Ok(filtered_entries)
            },
            Some(_) => Err("ERR WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            None => Ok(vec![]),
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

    pub fn get_type(&self, key: &str) -> String {
        let data = self.data.lock().unwrap();
        match data.get(key) {
            Some(ValueWrapper::String { .. }) => "string".to_string(),
            Some(ValueWrapper::Stream { .. }) => "stream".to_string(),
            None => "none".to_string(),
        }
    }
}
