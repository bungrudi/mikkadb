use dashmap::DashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use std::borrow::Cow;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct StreamEntry {
    pub id: String,
    pub fields: HashMap<String, String>,
}

#[derive(Default, Clone)]
pub struct StreamMetadata {
    pub last_sequences: DashMap<u64, u64>,
    pub last_dollar_id: Option<String>,
}

#[derive(Clone)]
pub enum ValueWrapper {
    String {
        value: String,
        expiration: Option<u64>,
    },
    Stream {
        entries: Vec<StreamEntry>,
        metadata: StreamMetadata,
    },
    List {
        values: Vec<String>,
    },
}

pub struct Storage {
    data: DashMap<String, ValueWrapper>,
}

impl Storage {
    pub fn new() -> Self {
        Storage {
            data: DashMap::new(),
        }
    }

    pub fn flushdb(&self) {
        self.data.clear();
    }

    pub fn set(&self, key: &str, value: &str, ttl: Option<usize>) {
        let expiration = ttl.map(|ttl| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64
                + ttl as u64
        });
        #[cfg(debug_assertions)]
        println!("DEBUG: Setting key '{}' with value '{}' and expiration {:?}", key, value, expiration);
        self.data.insert(
            key.to_string(),
            ValueWrapper::String {
                value: value.to_string(),
                expiration,
            },
        );
    }

   pub fn get(&self, key: &str) -> Option<String> {
        if let Some(entry) = self.data.get(key) {
            match &*entry {
                ValueWrapper::String { value, expiration } => {
                    if let Some(expiration) = expiration {
                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_millis() as u64;
                        if now > *expiration {
                            #[cfg(debug_assertions)]
                            println!("DEBUG: Key '{}' has expired. Current time: {}, Expiration: {}", key, now, expiration);
                            drop(entry); // Release the entry lock before removing the key
                            self.data.remove(key);
                            return None;
                        }
                    }
                    #[cfg(debug_assertions)]
                    println!("DEBUG: Retrieved key '{}' with value '{}'", key, value);
                    Some(value.clone())
                },
                ValueWrapper::Stream { .. } => None,
                ValueWrapper::List { .. } => None,
            }
        } else {
            #[cfg(debug_assertions)]
            println!("DEBUG: Key '{}' not found", key);
            None
        }
    }

   pub fn lpush(&self, key: &str, value: &str) -> Result<i64, String> {
        match self.data.entry(key.to_string()) {
            dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                match entry.get_mut() {
                    ValueWrapper::List { values } => {
                        values.insert(0, value.to_string());
                        Ok(values.len() as i64)
                    },
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                }
            },
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(ValueWrapper::List {
                    values: vec![value.to_string()],
                });
                Ok(1)
            },
        }
    }

   pub fn rpush(&self, key: &str, value: &str) -> Result<i64, String> {
        match self.data.entry(key.to_string()) {
            dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                match entry.get_mut() {
                    ValueWrapper::List { values } => {
                        values.push(value.to_string());
                        Ok(values.len() as i64)
                    },
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                }
            },
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(ValueWrapper::List {
                    values: vec![value.to_string()],
                });
                Ok(1)
            },
        }
    }

   pub fn lpop(&self, key: &str) -> Option<String> {
        self.data.get_mut(key).and_then(|mut entry| {
            if let ValueWrapper::List { values } = entry.value_mut() {
                (!values.is_empty()).then(|| values.remove(0))
            } else {
                None
            }
        })
    }

   pub fn rpop(&self, key: &str) -> Option<String> {
        self.data.get_mut(key).and_then(|mut entry| {
            if let ValueWrapper::List { values } = entry.value_mut() {
                values.pop()
            } else {
                None
            }
        })
    }

   pub fn llen(&self, key: &str) -> i64 {
        match self.data.get(key) {
            Some(entry) => {
                if let ValueWrapper::List { values } = &*entry {
                    values.len() as i64
                } else {
                    0
                }
            },
            None => 0,
        }
    }

   pub fn lrange(&self, key: &str, start: i64, stop: i64) -> Vec<String> {
        if let Some(entry) = self.data.get(key) {
            if let ValueWrapper::List { values } = &*entry {
                let len = values.len() as i64;
                if len == 0 {
                    return vec![];
                }
                let (start, stop) = self.normalize_indices(start, stop, len);
                if start >= stop {
                    return vec![];
                }
                values[start..stop].to_vec()
            } else {
                vec![]
            }
        } else {
            vec![]
        }
    }

   pub fn ltrim(&mut self, key: &str, start: i64, stop: i64) -> Result<(), String> {
        if let Some(mut entry) = self.data.get_mut(key) {
            if let ValueWrapper::List { values } = entry.value_mut() {
                let len = values.len() as i64;
                let (start_idx, stop_idx) = self.normalize_indices(start, stop, len);
                if start_idx >= values.len() || start_idx >= stop_idx {
                    values.clear();
                } else {
                    // First remove elements from the end
                    if stop_idx < values.len() {
                        values.drain(stop_idx..);
                    }
                    // Then remove elements from the start
                    values.drain(..start_idx);
                }
                Ok(())
            } else {
                Err("ERR value is not a list".to_string())
            }
        } else {
            Ok(())
        }
    }

   pub fn lpos(&self, key: &str, element: &str, count: Option<usize>) -> Option<Vec<usize>> {
        if let Some(entry) = self.data.get(key) {
            if let ValueWrapper::List { values } = &*entry {
                let mut positions = Vec::new();
                for (i, val) in values.iter().enumerate() {
                    if val == element {
                        positions.push(i);
                        if let Some(c) = count {
                            if positions.len() >= c {
                                break;
                            }
                        } else {
                            break;  // If no count specified, only return first match
                        }
                    }
                }
                if positions.is_empty() {
                    None  // Element not found
                } else {
                    Some(positions)
                }
            } else {
                None
            }
        } else {
            None  // Key doesn't exist
        }
    }

    pub fn linsert(&self, key: &str, before: bool, pivot: &str, element: &str) -> Option<usize> {
        match self.data.get_mut(key) {
            Some(mut entry) => match entry.value_mut() {
                ValueWrapper::List { values } => {
                    if let Some(pos) = values.iter().position(|x| x == pivot) {
                        let insert_pos = if before { pos } else { pos + 1 };
                        values.insert(insert_pos, element.to_string());
                        Some(values.len())
                    } else {
                        Some(0)
                    }
                },
                _ => None,
            },
            None => Some(0),
        }
    }

    pub fn lset(&mut self, key: &str, index: i64, element: &str) -> Result<(), String> {
        if let Some(mut entry) = self.data.get_mut(key) {
            if let ValueWrapper::List { values } = entry.value_mut() {
                let len = values.len() as i64;
                let idx = if index < 0 { len + index } else { index };
                if idx < 0 || idx >= len {
                    return Err("ERR index out of range".to_string());
                }
                values[idx as usize] = element.to_string();
                Ok(())
            } else {
                Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
            }
        } else {
            Err("ERR no such key".to_string())
        }
    }

    pub fn lindex(&self, key: &str, index: i64) -> Option<String> {
        match self.data.get(key) {
            Some(entry) => match entry.value() {
                ValueWrapper::List { values } => {
                    let len = values.len() as i64;
                    let index = if index < 0 { len + index } else { index };
                    if index < 0 || index >= len {
                        None
                    } else {
                        Some(values[index as usize].clone())
                    }
                },
                _ => None,
            },
            None => None,
        }
    }

    pub fn incr(&self, key: &str) -> Result<i64, String> {
        match self.data.entry(key.to_string()) {
            dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                match entry.get_mut() {
                    ValueWrapper::String { value, .. } => {
                        match value.parse::<i64>() {
                            Ok(num) => {
                                let new_num = num + 1;
                                *value = new_num.to_string();
                                Ok(new_num)
                            },
                            Err(_) => Err("ERR value is not an integer or out of range".to_string()),
                        }
                    },
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                }
            },
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(ValueWrapper::String {
                    value: "1".to_string(),
                    expiration: None,
                });
                Ok(1)
            },
        }
    }

    pub fn compare_stream_ids(id1: &str, id2: &str) -> std::cmp::Ordering {
        #[cfg(debug_assertions)]
        println!("DEBUG: Comparing stream IDs: {} and {}", id1, id2);
        
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

    pub fn get_next_sequence(metadata: &mut StreamMetadata, time_part: u64) -> u64 {
        let next_seq = match metadata.last_sequences.get(&time_part) {
            Some(seq) => *seq + 1,
            None => if time_part == 0 { 1 } else { 0 }
        };
        metadata.last_sequences.insert(time_part, next_seq);
        next_seq
    }

    pub fn validate_new_id(&self, entries: &[StreamEntry], new_id: &str) -> Result<(), Cow<'static, str>> {
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

        match self.data.entry(key.to_string()) {
            dashmap::mapref::entry::Entry::Occupied(mut occupied) => {
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
                        Ok(new_id)
                    },
                    _ => Err("ERR WRONGTYPE Operation against a key holding the wrong kind of value".into()),
                }
            },
            dashmap::mapref::entry::Entry::Vacant(vacant) => {
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
        match self.data.get(key) {
            Some(entry) => match entry.value() {
                ValueWrapper::Stream { entries, .. } => {
                    let filtered_entries: Vec<StreamEntry> = entries
                        .iter()
                        .filter(|entry| {
                            Self::compare_stream_ids(&entry.id, start) != std::cmp::Ordering::Less &&
                            Self::compare_stream_ids(&entry.id, end) != std::cmp::Ordering::Greater
                        })
                        .cloned()
                        .collect();
                    Ok(filtered_entries)
                },
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".into()),
            },
            None => Ok(Vec::new()),
        }
    }

    pub fn get_stream_entries(&self, stream_key: &str, ms: u64, seq: u64, count: Option<usize>) -> Vec<StreamEntry> {
        match self.data.get(stream_key) {
            Some(entry) => match entry.value() {
                ValueWrapper::Stream { entries, .. } => {
                    let search_id = format!("{}-{}", ms, seq);
                    let mut matching_entries: Vec<StreamEntry> = entries
                        .iter()
                        .filter(|entry| Self::compare_stream_ids(&entry.id, &search_id) == std::cmp::Ordering::Greater)
                        .cloned()
                        .collect();

                    matching_entries.sort_by(|a, b| Self::compare_stream_ids(&a.id, &b.id));
                    
                    if let Some(count) = count {
                        matching_entries.truncate(count);
                    }
                    
                    matching_entries
                },
                _ => Vec::new(),
            },
            None => Vec::new(),
        }
    }

    pub fn parse_stream_id(id: &str) -> Result<(u64, u64), String> {
        let parts: Vec<&str> = id.split('-').collect();
        if parts.len() != 2 {
            return Err("ERR Invalid stream ID format".to_string());
        }
        
        let ms = parts[0].parse::<u64>()
            .map_err(|_| "ERR Invalid stream ID milliseconds")?;
        let seq = parts[1].parse::<u64>()
            .map_err(|_| "ERR Invalid stream ID sequence number")?;
            
        Ok((ms, seq))
    }

    pub fn keys(&self, pattern: &str) -> Vec<String> {
        let mut keys = Vec::new();
        for entry in self.data.iter() {
            let key = entry.key();
            if pattern == "*" || key == pattern {
                keys.push(key.clone());
            }
        }
        keys
    }

    pub fn get_type(&self, key: &str) -> Cow<'static, str> {
        match self.data.get(key) {
            Some(entry) => match entry.value() {
                ValueWrapper::String { .. } => "string".into(),
                ValueWrapper::Stream { .. } => "stream".into(),
                ValueWrapper::List { .. } => "list".into(),
            },
            None => "none".into(),
        }
    }

    pub fn get_last_stream_id(&self, stream_key: &str) -> Option<String> {
        match self.data.get(stream_key) {
            Some(entry) => match entry.value() {
                ValueWrapper::Stream { entries, metadata } => {
                    if entries.is_empty() {
                        metadata.last_dollar_id.clone()
                    } else {
                        Some(entries.last()?.id.clone())
                    }
                },
                _ => None,
            },
            None => None,
        }
    }

    pub fn normalize_indices(&self, start: i64, stop: i64, len: i64) -> (usize, usize) {
        let start_idx = if start < 0 { len + start } else { start };
        let stop_idx = if stop < 0 { len + stop } else { stop };
        let start_idx = start_idx.max(0) as usize;
        let stop_idx = (stop_idx.min(len - 1) + 1) as usize;  // +1 to make range exclusive
        (start_idx, stop_idx)
    }
}
