use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};
use std::borrow::Cow;

#[derive(Debug, Clone)]
pub struct StreamEntry {
    pub id: String,
    pub fields: HashMap<String, String>,
}

#[derive(Default, Clone)]
pub struct StreamMetadata {
    pub last_sequences: HashMap<u64, u64>,
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
    data: Mutex<HashMap<String, ValueWrapper>>,
}

impl Storage {
    pub fn new() -> Self {
        Storage {
            data: Mutex::new(HashMap::new()),
        }
    }

    pub fn flushdb(&self) {
        let mut data = self.data.lock().unwrap();
        data.clear();
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
                ValueWrapper::List { .. } => None,
            }
        } else {
            #[cfg(debug_assertions)]
            println!("DEBUG: Key '{}' not found", key);
            None
        }
    }

    pub fn lpush(&self, key: &str, value: &str) -> Result<i64, String> {
        let mut data = self.data.lock().unwrap();
        match data.entry(key.to_string()) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                match entry.get_mut() {
                    ValueWrapper::List { values } => {
                        values.insert(0, value.to_string());
                        Ok(values.len() as i64)
                    },
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                }
            },
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(ValueWrapper::List {
                    values: vec![value.to_string()],
                });
                Ok(1)
            },
        }
    }

    pub fn rpush(&self, key: &str, value: &str) -> Result<i64, String> {
        let mut data = self.data.lock().unwrap();
        match data.entry(key.to_string()) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                match entry.get_mut() {
                    ValueWrapper::List { values } => {
                        values.push(value.to_string());
                        Ok(values.len() as i64)
                    },
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                }
            },
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(ValueWrapper::List {
                    values: vec![value.to_string()],
                });
                Ok(1)
            },
        }
    }

    pub fn lpop(&self, key: &str) -> Option<String> {
        let mut data = self.data.lock().unwrap();
        if let Some(wrapper) = data.get_mut(key) {
            match wrapper {
                ValueWrapper::List { values } => {
                    if values.is_empty() {
                        None
                    } else {
                        Some(values.remove(0))
                    }
                },
                _ => None,
            }
        } else {
            None
        }
    }

    pub fn rpop(&self, key: &str) -> Option<String> {
        let mut data = self.data.lock().unwrap();
        if let Some(wrapper) = data.get_mut(key) {
            match wrapper {
                ValueWrapper::List { values } => {
                    if values.is_empty() {
                        None
                    } else {
                        values.pop()
                    }
                },
                _ => None,
            }
        } else {
            None
        }
    }

    pub fn llen(&self, key: &str) -> i64 {
        let data = self.data.lock().unwrap();
        match data.get(key) {
            Some(ValueWrapper::List { values }) => values.len() as i64,
            _ => 0,
        }
    }

    pub fn lrange(&self, key: &str, start: i64, stop: i64) -> Vec<String> {
        let data = self.data.lock().unwrap();
        if let Some(ValueWrapper::List { values }) = data.get(key) {
            let len = values.len() as i64;
            let (start, stop) = normalize_indices(start, stop, len);
            values[start as usize..=stop as usize].to_vec()
        } else {
            vec![]
        }
    }

    pub fn ltrim(&self, key: &str, start: i64, stop: i64) -> Result<(), String> {
        let mut data = self.data.lock().unwrap();
        if let Some(wrapper) = data.get_mut(key) {
            match wrapper {
                ValueWrapper::List { values } => {
                    let len = values.len() as i64;
                    let (start, stop) = normalize_indices(start, stop, len);
                    *values = values[start as usize..=stop as usize].to_vec();
                    Ok(())
                },
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            }
        } else {
            Ok(())
        }
    }

    pub fn lpos(&self, key: &str, element: &str, count: Option<i64>) -> Vec<i64> {
        let data = self.data.lock().unwrap();
        if let Some(ValueWrapper::List { values }) = data.get(key) {
            let mut positions = Vec::new();
            for (i, value) in values.iter().enumerate() {
                if value == element {
                    positions.push(i as i64);
                    if let Some(count) = count {
                        if positions.len() == count as usize {
                            break;
                        }
                    }
                }
            }
            positions
        } else {
            vec![]
        }
    }

    pub fn linsert(&self, key: &str, before: bool, pivot: &str, element: &str) -> Result<i64, String> {
        let mut data = self.data.lock().unwrap();
        if let Some(wrapper) = data.get_mut(key) {
            match wrapper {
                ValueWrapper::List { values } => {
                    if let Some(pos) = values.iter().position(|x| x == pivot) {
                        if before {
                            values.insert(pos, element.to_string());
                        } else {
                            values.insert(pos + 1, element.to_string());
                        }
                        Ok(values.len() as i64)
                    } else {
                        Ok(-1)
                    }
                },
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            }
        } else {
            Ok(0)
        }
    }

    pub fn lset(&self, key: &str, index: i64, element: &str) -> Result<(), String> {
        let mut data = self.data.lock().unwrap();
        if let Some(wrapper) = data.get_mut(key) {
            match wrapper {
                ValueWrapper::List { values } => {
                    let len = values.len() as i64;
                    let real_index = if index < 0 { len + index } else { index };
                    if real_index < 0 || real_index >= len {
                        return Err("ERR index out of range".to_string());
                    }
                    values[real_index as usize] = element.to_string();
                    Ok(())
                },
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            }
        } else {
            Err("ERR no such key".to_string())
        }
    }

    pub fn lindex(&self, key: &str, index: i64) -> Option<String> {
        let data = self.data.lock().unwrap();
        if let Some(ValueWrapper::List { values }) = data.get(key) {
            let len = values.len() as i64;
            let real_index = if index < 0 { len + index } else { index };
            if real_index < 0 || real_index >= len {
                None
            } else {
                Some(values[real_index as usize].clone())
            }
        } else {
            None
        }
    }

    pub fn incr(&self, key: &str) -> Result<i64, String> {
        let mut data = self.data.lock().unwrap();
        
        let wrapper = match data.get(key) {
            Some(w) => w.clone(),  // Clone the wrapper to avoid borrow issues
            None => {
                // Key doesn't exist - create it with value "1"
                data.insert(
                    key.to_string(), 
                    ValueWrapper::String {
                        value: "1".to_string(),
                        expiration: None,
                    }
                );
                return Ok(1);
            }
        };

        // Process existing value
        match wrapper {
            ValueWrapper::String { value, expiration } => {
                match value.parse::<i64>() {
                    Ok(num) => {
                        let new_value = num + 1;
                        data.insert(
                            key.to_string(),
                            ValueWrapper::String {
                                value: new_value.to_string(),
                                expiration,
                            },
                        );
                        Ok(new_value)
                    },
                    Err(_) => Err("ERR value is not an integer or out of range".to_string()),
                }
            },
            ValueWrapper::Stream { .. } | ValueWrapper::List { .. } => {
                Err("ERR value is not an integer or out of range".to_string())
            },
        }
    }

    fn compare_stream_ids(id1: &str, id2: &str) -> std::cmp::Ordering {
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

    fn validate_stream_id(id: &str) -> Result<(), Cow<'static, str>> {
        if id == "$" || id == "0" {
            return Ok(());
        }
        let parts: Vec<&str> = id.split('-').collect();
        if parts.len() != 2 {
            return Err("ERR Invalid milliseconds in stream ID".into());
        }

        if let Err(_) = parts[0].parse::<u64>() {
            return Err("ERR Invalid milliseconds in stream ID".into());
        }

        if let Err(_) = parts[1].parse::<u64>() {
            return Err("ERR Invalid milliseconds in stream ID".into());
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
        };

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

    pub fn get_stream_entries(&self, stream_key: &str, ms: u64, seq: u64, count: Option<usize>) -> Vec<StreamEntry> {
        let data = self.data.lock().unwrap();
        if let Some(ValueWrapper::Stream { entries, .. }) = data.get(stream_key) {
            let mut matching_entries: Vec<StreamEntry> = entries
                .iter()
                .filter(|entry| {
                    if let Ok((entry_ms, entry_seq)) = Self::parse_stream_id(&entry.id) {
                        (entry_ms > ms) || (entry_ms == ms && entry_seq >= seq)
                    } else {
                        false
                    }
                })
                .cloned()
                .collect();

            matching_entries.sort_by(|a, b| Storage::compare_stream_ids(&a.id, &b.id));

            if let Some(count) = count {
                matching_entries.truncate(count);
            }

            matching_entries
        } else {
            vec![]
        }
    }

    pub fn parse_stream_id(id: &str) -> Result<(u64, u64), String> {
        if id == "$" {
            return Ok((u64::MAX, 0));
        }

        let parts: Vec<&str> = id.split('-').collect();
        match parts.len() {
            1 => {
                let ms = parts[0].parse::<u64>()
                    .map_err(|_| format!("Invalid stream ID format: {}", id))?;
                Ok((ms, 0))
            }
            2 => {
                let ms = parts[0].parse::<u64>()
                    .map_err(|_| format!("Invalid stream ID format: {}", id))?;
                let seq = parts[1].parse::<u64>()
                    .map_err(|_| format!("Invalid stream ID format: {}", id))?;
                Ok((ms, seq))
            }
            _ => Err(format!("Invalid stream ID format: {}", id))
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
            Some(ValueWrapper::List { .. }) => "list".into(),
            None => "none".into(),
        }
    }

    pub fn get_last_stream_id(&self, stream_key: &str) -> Option<String> {
        let data = self.data.lock().unwrap();
        if let Some(ValueWrapper::Stream { entries, metadata }) = data.get(stream_key) {
            if let Some(last_id) = &metadata.last_dollar_id {
                return Some(last_id.clone());
            }
            
            if !entries.is_empty() {
                let mut last_entry = entries[0].id.clone();
                for entry in entries.iter().skip(1) {
                    if Self::compare_stream_ids(&entry.id, &last_entry) == std::cmp::Ordering::Greater {
                        last_entry = entry.id.clone();
                    }
                }
                return Some(last_entry);
            }
        }
        None
    }
}

fn normalize_indices(start: i64, stop: i64, len: i64) -> (i64, i64) {
    let start = if start < 0 { len + start } else { start };
    let stop = if stop < 0 { len + stop } else { stop };
    let start = start.max(0).min(len - 1);
    let stop = stop.max(0).min(len - 1);
    (start, stop)
}
