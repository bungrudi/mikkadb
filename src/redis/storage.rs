use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};
use std::borrow::Cow;

#[derive(Debug)]
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
        #[cfg(debug_assertions)]
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
                            #[cfg(debug_assertions)]
                            println!("DEBUG: Key '{}' has expired. Current time: {}, Expiration: {}", key, now, expiration);
                            data.remove(key);
                            return None;
                        }
                    }
                    #[cfg(debug_assertions)]
                    println!("DEBUG: Retrieved key '{}' with value '{}'", key, value);
                    Some(value.clone())
                },
                ValueWrapper::Stream { .. } => None,
            }
        } else {
            #[cfg(debug_assertions)]
            println!("DEBUG: Key '{}' not found", key);
            None
        }
    }

    fn compare_stream_ids(id1: &str, id2: &str) -> std::cmp::Ordering {
        #[cfg(debug_assertions)]
        println!("DEBUG: Comparing stream IDs: {} and {}", id1, id2);
        
        if id1 == "-" {
            #[cfg(debug_assertions)]
            println!("DEBUG: First ID is '-', returning Less");
            return std::cmp::Ordering::Less;
        }
        if id2 == "-" {
            #[cfg(debug_assertions)]
            println!("DEBUG: Second ID is '-', returning Greater");
            return std::cmp::Ordering::Greater;
        }
        if id1 == "+" {
            #[cfg(debug_assertions)]
            println!("DEBUG: First ID is '+', returning Greater");
            return std::cmp::Ordering::Greater;
        }
        if id2 == "+" {
            #[cfg(debug_assertions)]
            println!("DEBUG: Second ID is '+', returning Less");
            return std::cmp::Ordering::Less;
        }

        let parts1: Vec<&str> = id1.split('-').collect();
        let parts2: Vec<&str> = id2.split('-').collect();

        let ms1 = parts1[0].parse::<u64>().unwrap();
        let ms2 = parts2[0].parse::<u64>().unwrap();
        #[cfg(debug_assertions)]
        println!("DEBUG: Comparing timestamps: {} and {}", ms1, ms2);

        if ms1 != ms2 {
            let result = ms1.cmp(&ms2);
            #[cfg(debug_assertions)]
            println!("DEBUG: Timestamps differ, returning {:?}", result);
            result
        } else {
            let seq1 = parts1[1].parse::<u64>().unwrap();
            let seq2 = parts2[1].parse::<u64>().unwrap();
            #[cfg(debug_assertions)]
            println!("DEBUG: Comparing sequences: {} and {}", seq1, seq2);
            let result = seq1.cmp(&seq2);
            #[cfg(debug_assertions)]
            println!("DEBUG: Sequences comparison result: {:?}", result);
            result
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
        #[cfg(debug_assertions)]
        println!("DEBUG: XADD acquiring lock...");
        let mut data = self.data.lock().unwrap();
        #[cfg(debug_assertions)]
        println!("DEBUG: XADD acquired lock");
        
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
                #[cfg(debug_assertions)]
                println!("DEBUG: XADD releasing lock (error case)");
                return Err("ERR The ID specified in XADD must be greater than 0-0".into());
            }
        }

        let result = match data.entry(key.to_string()) {
            std::collections::hash_map::Entry::Occupied(mut occupied) => {
                match occupied.get_mut() {
                    ValueWrapper::Stream { entries, metadata } => {
                        let new_sequence = sequence.unwrap_or_else(|| Self::get_next_sequence(metadata, time_part));
                        let new_id = format!("{}-{}", time_part, new_sequence);
                        
                        if let Err(e) = self.validate_new_id(entries, &new_id) {
                            return Err(e);
                        }

                        let entry = StreamEntry {
                            id: new_id.clone(),
                            fields,
                        };
                        entries.push(entry);
                        #[cfg(debug_assertions)]
                        println!("DEBUG: XADD added new entry with id: {}", new_id);
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
                #[cfg(debug_assertions)]
                println!("DEBUG: XADD created new stream with id: {}", new_id);
                Ok(new_id)
            },
        };

        #[cfg(debug_assertions)]
        println!("DEBUG: XADD releasing lock");
        result
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
                    .map(|entry| StreamEntry {
                        id: entry.id.clone(),
                        fields: entry.fields.clone(),
                    })
                    .collect();
                Ok(filtered_entries)
            },
            Some(_) => Err("ERR WRONGTYPE Operation against a key holding the wrong kind of value".into()),
            None => Ok(vec![]),
        }
    }

    pub fn xread(&self, keys: &[&str], ids: &[&str], block: Option<u64>) -> Result<Vec<(String, Vec<StreamEntry>)>, Cow<'static, str>> {
        #[cfg(debug_assertions)]
        println!("DEBUG: XREAD called with keys: {:?}, ids: {:?}, block: {:?}", keys, ids, block);
        let mut result = Vec::new();

        // Check for new entries
        for (&key, &id) in keys.iter().zip(ids.iter()) {
            #[cfg(debug_assertions)]
            println!("DEBUG: XREAD acquiring lock for key: {}", key);
            let entries = {
                let data = self.data.lock().unwrap();
                #[cfg(debug_assertions)]
                println!("DEBUG: XREAD acquired lock for key: {}", key);
                
                let entries = match data.get(key) {
                    Some(ValueWrapper::Stream { entries, .. }) => {
                        #[cfg(debug_assertions)]
                        println!("DEBUG: XREAD found stream for key: {} with {} entries", key, entries.len());
                        entries.iter()
                            .filter(|entry| {
                                let comparison = Self::compare_stream_ids(&entry.id, id);
                                #[cfg(debug_assertions)]
                                println!("DEBUG: XREAD comparing entry {} with last_id {}: {:?}", entry.id, id, comparison);
                                comparison > std::cmp::Ordering::Equal
                            })
                            .map(|entry| StreamEntry {
                                id: entry.id.clone(),
                                fields: entry.fields.clone(),
                            })
                            .collect::<Vec<StreamEntry>>()
                    },
                    Some(_) => return Err("ERR WRONGTYPE Operation against a key holding the wrong kind of value".into()),
                    None => {
                        #[cfg(debug_assertions)]
                        println!("DEBUG: XREAD no stream found for key: {}", key);
                        Vec::new()
                    },
                };
                #[cfg(debug_assertions)]
                println!("DEBUG: XREAD releasing lock for key: {}", key);
                entries
            };

            if !entries.is_empty() {
                #[cfg(debug_assertions)]
                println!("DEBUG: XREAD found {} new entries for key: {}", entries.len(), key);
                result.push((key.to_string(), entries));
            } else {
                #[cfg(debug_assertions)]
                println!("DEBUG: XREAD found no new entries for key: {}", key);
            }
        }

        if !result.is_empty() {
            #[cfg(debug_assertions)]
            println!("DEBUG: XREAD returning results immediately");
            return Ok(result);
        }

        match block {
            Some(timeout) => {
                #[cfg(debug_assertions)]
                println!("DEBUG: XREAD requesting retry with timeout: {}", timeout);
                Err(format!("{} {}", crate::redis::XREAD_RETRY_PREFIX, timeout).into())
            }
            None => {
                #[cfg(debug_assertions)]
                println!("DEBUG: XREAD returning empty result (non-blocking)");
                Ok(vec![])
            }
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

    pub fn get_type(&self, key: &str) -> Cow<'static, str> {
        let data = self.data.lock().unwrap();
        match data.get(key) {
            Some(ValueWrapper::String { .. }) => "string".into(),
            Some(ValueWrapper::Stream { .. }) => "stream".into(),
            None => "none".into(),
        }
    }
}
