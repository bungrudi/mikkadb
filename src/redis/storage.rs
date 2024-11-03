use std::collections::{HashMap, BTreeMap};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};
use std::borrow::Cow;

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
        if id1 == "-" {
            return std::cmp::Ordering::Less;
        }
        if id2 == "-" {
            return std::cmp::Ordering::Greater;
        }
        if id1 == "+" {
            return std::cmp::Ordering::Greater;
        }
        if id2 == "+" {
            return std::cmp::Ordering::Less;
        }

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

    fn validate_new_id(&self, entries: &[StreamEntry], new_id: &str) -> Result<(), Cow<'static, str>> {
        if let Some(last_entry) = entries.last() {
            if Self::compare_stream_ids(new_id, &last_entry.id) != std::cmp::Ordering::Greater {
                return Err("ERR The ID specified in XADD is equal or smaller than the target stream top item".into());
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

    pub fn xadd(&self, key: &str, id: &str, fields: HashMap<String, String>) -> Result<String, Cow<'static, str>> {
        let mut data = self.data.lock().unwrap();
        
        let (time_part, sequence) = if id == "*" {
            (Self::get_current_time_ms(), None)
        } else {
            let parts: Vec<&str> = id.split('-').collect();
            let time = parts[0].parse::<u64>().unwrap();
            let is_auto_seq = parts[1] == "*";
            (time, if is_auto_seq { None } else { Some(parts[1].parse::<u64>().unwrap()) })
        };

        if id != "*" && !id.ends_with("-*") {
            let new_id = format!("{}-{}", time_part, sequence.unwrap_or(0));
            if Self::compare_stream_ids(&new_id, "0-0") != std::cmp::Ordering::Greater {
                return Err("ERR The ID specified in XADD must be greater than 0-0".into());
            }
        }

        match data.entry(key.to_string()) {
            std::collections::hash_map::Entry::Occupied(mut occupied) => {
                match occupied.get_mut() {
                    ValueWrapper::Stream { entries, metadata } => {
                        let new_sequence = sequence.unwrap_or_else(|| Self::get_next_sequence(metadata, time_part));
                        let new_id = format!("{}-{}", time_part, new_sequence);
                        
                        self.validate_new_id(entries, &new_id)?;

                        let entry = StreamEntry {
                            id: new_id.clone(),
                            fields,
                        };
                        entries.push(entry);
                        Ok(new_id)
                    },
                    _ => Err("ERR WRONGTYPE Operation against a key holding the wrong kind of value".into()),
                }
            },
            std::collections::hash_map::Entry::Vacant(vacant) => {
                let mut metadata = StreamMetadata::default();
                let new_sequence = sequence.unwrap_or_else(|| Self::get_next_sequence(&mut metadata, time_part));
                let new_id = format!("{}-{}", time_part, new_sequence);

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

    pub fn xrange(&self, key: &str, start: &str, end: &str) -> Result<Vec<StreamEntry>, Cow<'static, str>> {
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
            Some(_) => Err("ERR WRONGTYPE Operation against a key holding the wrong kind of value".into()),
            None => Ok(vec![]),
        }
    }

    pub fn xread(&self, keys: &[&str], ids: &[&str]) -> Result<Vec<(String, Vec<StreamEntry>)>, Cow<'static, str>> {
        let data = self.data.lock().unwrap();
        let mut result = Vec::new();
        
        for (&key, &id) in keys.iter().zip(ids.iter()) {
            match data.get(key) {
                Some(ValueWrapper::Stream { entries, .. }) => {
                    let filtered_entries: Vec<StreamEntry> = entries.iter()
                        .filter(|entry| {
                            Self::compare_stream_ids(&entry.id, id) == std::cmp::Ordering::Greater
                        })
                        .map(|entry| {
                            let ordered_fields: BTreeMap<_, _> = entry.fields.iter()
                                .map(|(k, v)| (k.as_str(), v.as_str()))
                                .collect();
                            StreamEntry {
                                id: entry.id.clone(),
                                fields: ordered_fields.into_iter()
                                    .map(|(k, v)| (k.to_owned(), v.to_owned()))
                                    .collect(),
                            }
                        })
                        .collect();
                    result.push((key.to_string(), filtered_entries));
                },
                Some(_) => return Err("ERR WRONGTYPE Operation against a key holding the wrong kind of value".into()),
                None => result.push((key.to_string(), vec![])),
            }
        }
        Ok(result)
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

    pub fn get_type(&self, key: &str) -> Cow<'static, str> {
        let data = self.data.lock().unwrap();
        match data.get(key) {
            Some(ValueWrapper::String { .. }) => "string".into(),
            Some(ValueWrapper::Stream { .. }) => "stream".into(),
            None => "none".into(),
        }
    }
}
