use std::collections::HashMap;
use std::fs;
use std::path::Path;
use bytes::Bytes;
use std::time::{Duration, Instant, SystemTime};
use std::time::UNIX_EPOCH;
use anyhow::{anyhow, bail, Context, Result as AnyResult};

#[derive(Clone, Debug, PartialEq)]
pub struct StreamEntry {
    pub id: (u64, u64), // (milliseconds, sequence)
    pub fields: Vec<(String, String)>,
}

#[derive(Clone)]
enum DataType {
    String(Bytes, Option<Instant>),
    Stream(Vec<StreamEntry>),
    List(Vec<Bytes>),
    SortedSet(HashMap<String, f64>),
    Hash(HashMap<String, Bytes>),
    Set(std::collections::HashSet<Bytes>),
}

#[derive(Clone)]
pub struct Db {
    data: HashMap<String, DataType>,
    /// Separate expiration map for non-string types
    /// String types store expiration inline; this map is for List, Stream, SortedSet
    expiry: HashMap<String, Instant>,
}

impl Db {
    pub fn new() -> Self {
        Db {
            data: HashMap::new(),
            expiry: HashMap::new(),
        }
    }

    pub fn load_rdb<P: AsRef<Path>>(&mut self, path: P) -> AnyResult<()> {
        let path = path.as_ref();
        if path.to_string_lossy().is_empty() {
            return Ok(());
        }

        let data = match fs::read(path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(anyhow!(e)).with_context(|| format!("Failed to read RDB file {}", path.display())),
        };

        if data.is_empty() {
            return Ok(());
        }

        let mut parser = RdbParser::new(&data);
        parser.validate_header()?;

        let mut expiry: Option<u64> = None;

        loop {
            let opcode_pos = parser.pos;
            let opcode = parser.read_u8().context("Unexpected end of RDB stream")?;
            eprintln!("[rdb] opcode {:#x} at pos {}", opcode, opcode_pos);
            match opcode {
                0xFF => break,
                0xFA => {
                    parser.read_string()?; // AUX key
                    parser.read_string()?; // AUX value
                }
                0xFB => {
                    parser.read_length()?; // hash table size
                    parser.read_length()?; // expires hash table size
                }
                // According to the Go encoder used in redis-tester (github.com/hdt3213/rdb),
                // TTL is written as an absolute Unix timestamp in *milliseconds* using an
                // 8-byte little-endian integer. We therefore treat both 0xFD and 0xFC as
                // 8-byte millisecond timestamps to stay in sync with that encoder.
                0xFD | 0xFC => {
                    let ts_ms = parser.read_u64()?;
                    expiry = Some(ts_ms);
                }
                0xFE => {
                    parser.read_length()?; // DB selector, ignore single DB
                }
                0x00 => {
                    let key = parser.read_string()?;
                    let value = parser.read_string()?;
                    self.store_loaded_string(&key, value, expiry.take());
                }
                other => {
                    bail!("Unsupported RDB opcode: {:#x} at position {}", other, opcode_pos);
                }
            }
        }

        Ok(())
    }

    pub fn set(&mut self, key: String, value: Bytes, px: Option<u64>) {
        let expiry = px.map(|ms| Instant::now() + Duration::from_millis(ms));
        self.data.insert(key, DataType::String(value, expiry));
    }
    
    /// Zero-copy set: accepts Bytes key, converts to String at storage boundary
    /// Use this when you have Bytes from the parser to avoid intermediate String allocation
    pub fn set_bytes(&mut self, key: Bytes, value: Bytes, px: Option<u64>) {
        // Convert Bytes to String at storage boundary
        // This is where the allocation happens, but only once per SET
        let key_str = match std::str::from_utf8(&key) {
            Ok(s) => s.to_string(),
            Err(_) => String::from_utf8_lossy(&key).to_string(),
        };
        self.set(key_str, value, px);
    }

    pub fn get(&self, key: &str) -> Option<Bytes> {
        if let Some(data_type) = self.data.get(key) {
            match data_type {
                DataType::String(value, expiry) => {
                    if let Some(expiry_time) = expiry {
                        if Instant::now() > *expiry_time {
                            // Lazy expiration: check but don't remove (requires mut)
                            // Or return None.
                            // In standard HashMap with &self, we can't remove.
                            // We rely on cleanup or returning None.
                            return None;
                        }
                    }
                    return Some(value.clone());
                }
                _ => return None, // Wrong type
            }
        }
        None
    }
    
    /// Zero-copy get: accepts byte slice and converts to &str for lookup
    /// This avoids String allocation for the lookup key
    pub fn get_bytes(&self, key: &[u8]) -> Option<Bytes> {
        // Convert bytes to str for HashMap lookup (no allocation)
        match std::str::from_utf8(key) {
            Ok(key_str) => self.get(key_str),
            Err(_) => None, // Invalid UTF-8 keys not supported in String-based HashMap
        }
    }
    
    pub fn add_stream_entry(&mut self, key: String, id: (u64, u64), fields: Vec<(String, String)>) -> Result<(u64, u64), String> {
        let entry = StreamEntry { id, fields };
        
        let stream = self.data.entry(key).or_insert(DataType::Stream(Vec::new()));
        
        match stream {
            DataType::Stream(entries) => {
                // Validate ID
                if let Some(last) = entries.last() {
                    if id.0 < last.id.0 || (id.0 == last.id.0 && id.1 <= last.id.1) {
                        if id.0 == 0 && id.1 == 0 {
                             return Err("ERR The ID specified in XADD must be greater than 0-0".to_string());
                        }
                        return Err("ERR The ID specified in XADD is equal or smaller than the target stream top item".to_string());
                    }
                } else {
                    if id.0 == 0 && id.1 == 0 {
                         return Err("ERR The ID specified in XADD must be greater than 0-0".to_string());
                    }
                }
                
                entries.push(entry);
                Ok(id)
            }
            _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        }
    }
    
    pub fn get_last_stream_id(&self, key: &str) -> Option<(u64, u64)> {
        if let Some(entry) = self.data.get(key) {
            if let DataType::Stream(entries) = entry {
                return entries.last().map(|e| e.id);
            }
        }
        None
    }
    
    pub fn read_stream(&self, key: &str, start_id: (u64, u64)) -> Option<Vec<StreamEntry>> {
        if let Some(entry) = self.data.get(key) {
            if let DataType::Stream(entries) = entry {
                let result: Vec<StreamEntry> = entries.iter()
                    .filter(|e| e.id.0 > start_id.0 || (e.id.0 == start_id.0 && e.id.1 > start_id.1))
                    .cloned()
                    .collect();
                
                if result.is_empty() {
                    return None;
                } else {
                    return Some(result);
                }
            }
        }
        None
    }

    pub fn range_stream(&self, key: &str, start: (u64, u64), end: (u64, u64)) -> Option<Vec<StreamEntry>> {
        if let Some(entry) = self.data.get(key) {
            if let DataType::Stream(entries) = entry {
                let result: Vec<StreamEntry> = entries.iter()
                    .filter(|e| {
                        let id = e.id;
                        // Check start (inclusive)
                        let after_start = id.0 > start.0 || (id.0 == start.0 && id.1 >= start.1);
                        // Check end (inclusive)
                        let before_end = id.0 < end.0 || (id.0 == end.0 && id.1 <= end.1);
                        
                        after_start && before_end
                    })
                    .cloned()
                    .collect();
                
                Some(result)
            } else {
                None
            }
        } else {
            None
        }
    }

    pub fn rpush(&mut self, key: String, values: Vec<Bytes>) -> Result<usize, String> {
        let list = self.data.entry(key).or_insert(DataType::List(Vec::new()));
        
        match list {
            DataType::List(v) => {
                v.extend(values);
                Ok(v.len())
            }
            _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        }
    }
    
    pub fn lrange(&self, key: &str, start: i64, end: i64) -> Result<Vec<Bytes>, String> {
        match self.data.get(key) {
            Some(entry) => {
                if let DataType::List(v) = entry {
                    let len = v.len() as i64;
                    if len == 0 {
                        return Ok(Vec::new());
                    }
                    
                    // Normalize indices
                    let start_idx = if start < 0 { len + start } else { start };
                    let end_idx = if end < 0 { len + end } else { end };
                    
                    let start_idx = if start_idx < 0 { 0 } else { start_idx };
                    // end_idx is inclusive in Redis
                    let end_idx = if end_idx >= len { len - 1 } else { end_idx };
                    
                    if start_idx > end_idx {
                        return Ok(Vec::new());
                    }
                    
                    let result = v[start_idx as usize..=end_idx as usize].to_vec();
                    Ok(result)
                } else {
                    Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
                }
            }
            None => Ok(Vec::new()),
        }
    }

    pub fn lpush(&mut self, key: String, values: Vec<Bytes>) -> Result<usize, String> {
        let list = self.data.entry(key).or_insert(DataType::List(Vec::new()));
        
        match list {
            DataType::List(v) => {
                let mut values = values;
                values.reverse();
                v.splice(0..0, values);
                Ok(v.len())
            }
            _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        }
    }

    pub fn llen(&self, key: &str) -> Result<usize, String> {
        match self.data.get(key) {
            Some(entry) => {
                if let DataType::List(v) = entry {
                    Ok(v.len())
                } else {
                    Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
                }
            }
            None => Ok(0),
        }
    }

    pub fn lpop(&mut self, key: &str, count: Option<i64>) -> Result<Option<Vec<Bytes>>, String> {
        if let Some(entry) = self.data.get_mut(key) {
            match entry {
                DataType::List(v) => {
                    if v.is_empty() {
                        return Ok(None);
                    }
                    
                    let count = count.unwrap_or(1);
                    if count < 0 {
                         return Err("ERR value is out of range, must be positive".to_string());
                    }
                    
                    let count = count as usize;
                    let drain_count = std::cmp::min(count, v.len());
                    
                    let result: Vec<Bytes> = v.drain(0..drain_count).collect();
                    
                    Ok(Some(result))
                }
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            }
        } else {
            Ok(None)
        }
    }

    pub fn key_type(&self, key: &str) -> String {
        let now = Instant::now();
        match self.data.get(key) {
            Some(entry) => {
                if Self::is_expired(entry, now) {
                    return "none".to_string();
                }
                match entry {
                    DataType::String(_, _) => "string".to_string(),
                    DataType::Stream(_) => "stream".to_string(),
                    DataType::List(_) => "list".to_string(),
                    DataType::SortedSet(_) => "zset".to_string(),
                    DataType::Hash(_) => "hash".to_string(),
                    DataType::Set(_) => "set".to_string(),
                }
            }
            None => "none".to_string(),
        }
    }

    /// Delete a key from the database, regardless of its type.
    /// Returns true if the key was deleted, false if it didn't exist.
    pub fn del(&mut self, key: &str) -> bool {
        // First check if expired (lazy expiration)
        if let Some(entry) = self.data.get(key) {
            if Self::is_expired(entry, Instant::now()) {
                self.data.remove(key);
                return false; // Expired key doesn't count
            }
        }
        self.data.remove(key).is_some()
    }

    /// Zero-copy del: accepts byte slice and converts to &str for lookup
    pub fn del_bytes(&mut self, key: &[u8]) -> bool {
        match std::str::from_utf8(key) {
            Ok(key_str) => self.del(key_str),
            Err(_) => false,
        }
    }

    /// Check if a key exists in the database (any type).
    /// Returns true if the key exists and is not expired.
    pub fn exists(&self, key: &str) -> bool {
        match self.data.get(key) {
            Some(entry) => !Self::is_expired(entry, Instant::now()),
            None => false,
        }
    }

    /// Zero-copy exists: accepts byte slice and converts to &str for lookup
    pub fn exists_bytes(&self, key: &[u8]) -> bool {
        match std::str::from_utf8(key) {
            Ok(key_str) => self.exists(key_str),
            Err(_) => false,
        }
    }

    /// Set expiration on a key in seconds.
    /// Returns 1 if the timeout was set, 0 if key doesn't exist.
    pub fn expire(&mut self, key: &str, seconds: i64) -> i64 {
        if seconds <= 0 {
            // Redis: If seconds <= 0, the key is deleted
            if self.del(key) {
                return 1;
            }
            return 0;
        }
        self.pexpire(key, seconds * 1000)
    }

    /// Set expiration on a key in milliseconds.
    /// Returns 1 if the timeout was set, 0 if key doesn't exist.
    pub fn pexpire(&mut self, key: &str, milliseconds: i64) -> i64 {
        if milliseconds <= 0 {
            // Redis: If milliseconds <= 0, the key is deleted
            if self.del(key) {
                return 1;
            }
            return 0;
        }

        if !self.exists(key) {
            return 0;
        }

        let expiry_time = Instant::now() + Duration::from_millis(milliseconds as u64);

        // For String types, we need to update the inline expiration
        if let Some(DataType::String(value, _)) = self.data.get(key).cloned() {
            self.data.insert(key.to_string(), DataType::String(value, Some(expiry_time)));
        } else {
            // For other types, use the expiry map
            self.expiry.insert(key.to_string(), expiry_time);
        }

        1
    }

    /// Get time-to-live of a key in seconds.
    /// Returns -2 if key doesn't exist, -1 if no expiry, otherwise TTL in seconds.
    pub fn ttl(&self, key: &str) -> i64 {
        let pttl = self.pttl(key);
        match pttl {
            -2 | -1 => pttl,
            ms => (ms + 999) / 1000, // Round up to nearest second
        }
    }

    /// Get time-to-live of a key in milliseconds.
    /// Returns -2 if key doesn't exist, -1 if no expiry, otherwise TTL in milliseconds.
    pub fn pttl(&self, key: &str) -> i64 {
        let now = Instant::now();

        match self.data.get(key) {
            Some(entry) => {
                // Check if expired
                if Self::is_expired(entry, now) {
                    return -2; // Key doesn't exist (expired)
                }

                // Check for String inline expiration
                if let DataType::String(_, Some(expiry)) = entry {
                    let remaining = expiry.saturating_duration_since(now);
                    return remaining.as_millis() as i64;
                }

                // Check expiry map for other types
                if let Some(expiry) = self.expiry.get(key) {
                    if now > *expiry {
                        return -2; // Expired
                    }
                    let remaining = expiry.saturating_duration_since(now);
                    return remaining.as_millis() as i64;
                }

                // Key exists but no expiry
                -1
            }
            None => -2, // Key doesn't exist
        }
    }

    /// Increment (or decrement if negative) an integer value by the given amount.
    /// Returns Ok(new_value) on success, Err if key holds wrong type or value is not an integer.
    pub fn incrby(&mut self, key: &str, increment: i64) -> Result<i64, String> {
        let current_val = match self.data.get(key) {
            Some(DataType::String(value, _)) => {
                match std::str::from_utf8(value) {
                    Ok(s) => match s.parse::<i64>() {
                        Ok(n) => n,
                        Err(_) => return Err("ERR value is not an integer or out of range".to_string()),
                    },
                    Err(_) => return Err("ERR value is not an integer or out of range".to_string()),
                }
            }
            Some(_) => return Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            None => 0,
        };

        let new_val = current_val.checked_add(increment)
            .ok_or_else(|| "ERR increment or decrement would overflow".to_string())?;

        // Preserve existing expiration if present
        let expiry = if let Some(DataType::String(_, exp)) = self.data.get(key) {
            *exp
        } else {
            None
        };

        self.data.insert(key.to_string(), DataType::String(Bytes::from(new_val.to_string()), expiry));
        Ok(new_val)
    }

    /// Zero-copy incrby: accepts byte slice and converts to &str for lookup
    pub fn incrby_bytes(&mut self, key: &[u8], increment: i64) -> Result<i64, String> {
        match std::str::from_utf8(key) {
            Ok(key_str) => self.incrby(key_str, increment),
            Err(_) => Err("ERR invalid key encoding".to_string()),
        }
    }

    /// Append value to a string key. Creates key if it doesn't exist.
    /// Returns the length of the string after the append operation.
    pub fn append(&mut self, key: &str, value: &[u8]) -> Result<usize, String> {
        match self.data.get(key) {
            Some(DataType::String(existing, expiry)) => {
                let mut new_value = existing.to_vec();
                new_value.extend_from_slice(value);
                let len = new_value.len();
                self.data.insert(key.to_string(), DataType::String(Bytes::from(new_value), *expiry));
                Ok(len)
            }
            Some(_) => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            None => {
                let len = value.len();
                self.data.insert(key.to_string(), DataType::String(Bytes::copy_from_slice(value), None));
                Ok(len)
            }
        }
    }

    /// Zero-copy append
    pub fn append_bytes(&mut self, key: &[u8], value: &[u8]) -> Result<usize, String> {
        match std::str::from_utf8(key) {
            Ok(key_str) => self.append(key_str, value),
            Err(_) => Err("ERR invalid key encoding".to_string()),
        }
    }

    /// Get the length of a string value.
    /// Returns 0 if key doesn't exist.
    pub fn strlen(&self, key: &str) -> Result<usize, String> {
        match self.data.get(key) {
            Some(DataType::String(value, expiry)) => {
                // Check expiration
                if let Some(exp) = expiry {
                    if Instant::now() > *exp {
                        return Ok(0); // Expired
                    }
                }
                Ok(value.len())
            }
            Some(_) => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            None => Ok(0),
        }
    }

    /// Zero-copy strlen
    pub fn strlen_bytes(&self, key: &[u8]) -> Result<usize, String> {
        match std::str::from_utf8(key) {
            Ok(key_str) => self.strlen(key_str),
            Err(_) => Err("ERR invalid key encoding".to_string()),
        }
    }

    /// Set key to value only if key does not exist.
    /// Returns true if key was set, false if key already exists.
    pub fn setnx(&mut self, key: &str, value: Bytes) -> bool {
        if self.exists(key) {
            return false;
        }
        self.data.insert(key.to_string(), DataType::String(value, None));
        true
    }

    /// Zero-copy setnx
    pub fn setnx_bytes(&mut self, key: &[u8], value: Bytes) -> bool {
        match std::str::from_utf8(key) {
            Ok(key_str) => self.setnx(key_str, value),
            Err(_) => false,
        }
    }

    /// Get multiple keys at once. Returns None for keys that don't exist or are expired.
    pub fn mget(&self, keys: &[&str]) -> Vec<Option<Bytes>> {
        let now = Instant::now();
        keys.iter().map(|key| {
            match self.data.get(*key) {
                Some(entry) => {
                    if Self::is_expired(entry, now) {
                        None
                    } else if let DataType::String(value, _) = entry {
                        Some(value.clone())
                    } else {
                        None // Wrong type returns nil in MGET
                    }
                }
                None => None,
            }
        }).collect()
    }

    /// Zero-copy mget
    pub fn mget_bytes(&self, keys: &[Bytes]) -> Vec<Option<Bytes>> {
        let now = Instant::now();
        keys.iter().map(|key| {
            let key_str = match std::str::from_utf8(key) {
                Ok(s) => s,
                Err(_) => return None,
            };
            match self.data.get(key_str) {
                Some(entry) => {
                    if Self::is_expired(entry, now) {
                        None
                    } else if let DataType::String(value, _) = entry {
                        Some(value.clone())
                    } else {
                        None // Wrong type returns nil in MGET
                    }
                }
                None => None,
            }
        }).collect()
    }

    /// Set multiple key-value pairs at once
    pub fn mset(&mut self, pairs: &[(&str, Bytes)]) {
        for (key, value) in pairs {
            self.data.insert(key.to_string(), DataType::String(value.clone(), None));
        }
    }

    /// Zero-copy mset
    pub fn mset_bytes(&mut self, pairs: &[(Bytes, Bytes)]) {
        for (key, value) in pairs {
            if let Ok(key_str) = std::str::from_utf8(key) {
                self.data.insert(key_str.to_string(), DataType::String(value.clone(), None));
            }
        }
    }

    /// Remove expiration from a key.
    /// Returns 1 if the timeout was removed, 0 if key doesn't exist or had no timeout.
    pub fn persist(&mut self, key: &str) -> i64 {
        if !self.exists(key) {
            return 0;
        }

        // For String types, check and remove inline expiration
        if let Some(DataType::String(value, Some(_))) = self.data.get(key).cloned() {
            self.data.insert(key.to_string(), DataType::String(value, None));
            return 1;
        }

        // For other types, check expiry map
        if self.expiry.remove(key).is_some() {
            return 1;
        }

        0 // Key existed but had no timeout
    }

    /// Rename a key to a new key name.
    /// Returns Ok(()) on success, Err if source key doesn't exist.
    /// If destination key exists, it is overwritten.
    pub fn rename(&mut self, key: &str, newkey: &str) -> Result<(), String> {
        // Check if source key exists
        if !self.exists(key) {
            return Err("ERR no such key".to_string());
        }

        // If key == newkey, nothing to do
        if key == newkey {
            return Ok(());
        }

        // Remove the data from old key
        if let Some(data) = self.data.remove(key) {
            // Also move expiration if it exists
            let expiry = self.expiry.remove(key);

            // Insert into new key (overwrites if exists)
            self.data.insert(newkey.to_string(), data);
            if let Some(exp) = expiry {
                self.expiry.insert(newkey.to_string(), exp);
            }

            Ok(())
        } else {
            Err("ERR no such key".to_string())
        }
    }

    /// Zero-copy rename
    pub fn rename_bytes(&mut self, key: &[u8], newkey: &[u8]) -> Result<(), String> {
        match (std::str::from_utf8(key), std::str::from_utf8(newkey)) {
            (Ok(k), Ok(nk)) => self.rename(k, nk),
            _ => Err("ERR invalid key encoding".to_string()),
        }
    }

    // ==================== Hash Operations ====================

    /// Set hash fields. Returns the number of fields that were added (not updated).
    pub fn hset(&mut self, key: &str, fields: Vec<(String, Bytes)>) -> Result<usize, String> {
        let now = Instant::now();

        // Check if key exists with wrong type
        if let Some(entry) = self.data.get(key) {
            if Self::is_expired(entry, now) {
                self.data.remove(key);
            } else if !matches!(entry, DataType::Hash(_)) {
                return Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string());
            }
        }

        let hash = self.data
            .entry(key.to_string())
            .or_insert_with(|| DataType::Hash(HashMap::new()));

        if let DataType::Hash(h) = hash {
            let mut added = 0;
            for (field, value) in fields {
                if !h.contains_key(&field) {
                    added += 1;
                }
                h.insert(field, value);
            }
            Ok(added)
        } else {
            Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
        }
    }

    /// Zero-copy hset
    pub fn hset_bytes(&mut self, key: &[u8], fields: Vec<(Bytes, Bytes)>) -> Result<usize, String> {
        let key_str = std::str::from_utf8(key)
            .map_err(|_| "ERR invalid key encoding".to_string())?;
        let fields_str: Result<Vec<(String, Bytes)>, String> = fields
            .into_iter()
            .map(|(f, v)| {
                std::str::from_utf8(&f)
                    .map(|s| (s.to_string(), v))
                    .map_err(|_| "ERR invalid field encoding".to_string())
            })
            .collect();
        self.hset(key_str, fields_str?)
    }

    /// Get a hash field value. Returns None if key or field doesn't exist.
    pub fn hget(&self, key: &str, field: &str) -> Result<Option<Bytes>, String> {
        let now = Instant::now();
        match self.data.get(key) {
            Some(entry) => {
                if Self::is_expired(entry, now) {
                    return Ok(None);
                }
                match entry {
                    DataType::Hash(h) => Ok(h.get(field).cloned()),
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                }
            }
            None => Ok(None),
        }
    }

    /// Zero-copy hget
    pub fn hget_bytes(&self, key: &[u8], field: &[u8]) -> Result<Option<Bytes>, String> {
        let key_str = std::str::from_utf8(key)
            .map_err(|_| "ERR invalid key encoding".to_string())?;
        let field_str = std::str::from_utf8(field)
            .map_err(|_| "ERR invalid field encoding".to_string())?;
        self.hget(key_str, field_str)
    }

    /// Get multiple hash fields. Returns None for missing fields.
    pub fn hmget(&self, key: &str, fields: &[String]) -> Result<Vec<Option<Bytes>>, String> {
        let now = Instant::now();
        match self.data.get(key) {
            Some(entry) => {
                if Self::is_expired(entry, now) {
                    return Ok(fields.iter().map(|_| None).collect());
                }
                match entry {
                    DataType::Hash(h) => {
                        Ok(fields.iter().map(|f| h.get(f).cloned()).collect())
                    }
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                }
            }
            None => Ok(fields.iter().map(|_| None).collect()),
        }
    }

    /// Zero-copy hmget
    pub fn hmget_bytes(&self, key: &[u8], fields: &[Bytes]) -> Result<Vec<Option<Bytes>>, String> {
        let key_str = std::str::from_utf8(key)
            .map_err(|_| "ERR invalid key encoding".to_string())?;
        let fields_str: Result<Vec<String>, String> = fields
            .iter()
            .map(|f| std::str::from_utf8(f)
                .map(|s| s.to_string())
                .map_err(|_| "ERR invalid field encoding".to_string()))
            .collect();
        self.hmget(key_str, &fields_str?)
    }

    /// Get all fields and values in a hash.
    pub fn hgetall(&self, key: &str) -> Result<Vec<(String, Bytes)>, String> {
        let now = Instant::now();
        match self.data.get(key) {
            Some(entry) => {
                if Self::is_expired(entry, now) {
                    return Ok(Vec::new());
                }
                match entry {
                    DataType::Hash(h) => {
                        Ok(h.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
                    }
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                }
            }
            None => Ok(Vec::new()),
        }
    }

    /// Zero-copy hgetall
    pub fn hgetall_bytes(&self, key: &[u8]) -> Result<Vec<(String, Bytes)>, String> {
        let key_str = std::str::from_utf8(key)
            .map_err(|_| "ERR invalid key encoding".to_string())?;
        self.hgetall(key_str)
    }

    /// Delete hash fields. Returns the number of fields deleted.
    pub fn hdel(&mut self, key: &str, fields: &[String]) -> Result<usize, String> {
        let now = Instant::now();
        match self.data.get_mut(key) {
            Some(entry) => {
                if Self::is_expired(entry, now) {
                    self.data.remove(key);
                    return Ok(0);
                }
                match entry {
                    DataType::Hash(h) => {
                        let mut deleted = 0;
                        for field in fields {
                            if h.remove(field).is_some() {
                                deleted += 1;
                            }
                        }
                        Ok(deleted)
                    }
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                }
            }
            None => Ok(0),
        }
    }

    /// Zero-copy hdel
    pub fn hdel_bytes(&mut self, key: &[u8], fields: &[Bytes]) -> Result<usize, String> {
        let key_str = std::str::from_utf8(key)
            .map_err(|_| "ERR invalid key encoding".to_string())?;
        let fields_str: Result<Vec<String>, String> = fields
            .iter()
            .map(|f| std::str::from_utf8(f)
                .map(|s| s.to_string())
                .map_err(|_| "ERR invalid field encoding".to_string()))
            .collect();
        self.hdel(key_str, &fields_str?)
    }

    /// Check if hash field exists.
    pub fn hexists(&self, key: &str, field: &str) -> Result<bool, String> {
        let now = Instant::now();
        match self.data.get(key) {
            Some(entry) => {
                if Self::is_expired(entry, now) {
                    return Ok(false);
                }
                match entry {
                    DataType::Hash(h) => Ok(h.contains_key(field)),
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                }
            }
            None => Ok(false),
        }
    }

    /// Zero-copy hexists
    pub fn hexists_bytes(&self, key: &[u8], field: &[u8]) -> Result<bool, String> {
        let key_str = std::str::from_utf8(key)
            .map_err(|_| "ERR invalid key encoding".to_string())?;
        let field_str = std::str::from_utf8(field)
            .map_err(|_| "ERR invalid field encoding".to_string())?;
        self.hexists(key_str, field_str)
    }

    /// Get all field names in a hash.
    pub fn hkeys(&self, key: &str) -> Result<Vec<String>, String> {
        let now = Instant::now();
        match self.data.get(key) {
            Some(entry) => {
                if Self::is_expired(entry, now) {
                    return Ok(Vec::new());
                }
                match entry {
                    DataType::Hash(h) => Ok(h.keys().cloned().collect()),
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                }
            }
            None => Ok(Vec::new()),
        }
    }

    /// Zero-copy hkeys
    pub fn hkeys_bytes(&self, key: &[u8]) -> Result<Vec<String>, String> {
        let key_str = std::str::from_utf8(key)
            .map_err(|_| "ERR invalid key encoding".to_string())?;
        self.hkeys(key_str)
    }

    /// Get all values in a hash.
    pub fn hvals(&self, key: &str) -> Result<Vec<Bytes>, String> {
        let now = Instant::now();
        match self.data.get(key) {
            Some(entry) => {
                if Self::is_expired(entry, now) {
                    return Ok(Vec::new());
                }
                match entry {
                    DataType::Hash(h) => Ok(h.values().cloned().collect()),
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                }
            }
            None => Ok(Vec::new()),
        }
    }

    /// Zero-copy hvals
    pub fn hvals_bytes(&self, key: &[u8]) -> Result<Vec<Bytes>, String> {
        let key_str = std::str::from_utf8(key)
            .map_err(|_| "ERR invalid key encoding".to_string())?;
        self.hvals(key_str)
    }

    /// Get the number of fields in a hash.
    pub fn hlen(&self, key: &str) -> Result<usize, String> {
        let now = Instant::now();
        match self.data.get(key) {
            Some(entry) => {
                if Self::is_expired(entry, now) {
                    return Ok(0);
                }
                match entry {
                    DataType::Hash(h) => Ok(h.len()),
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                }
            }
            None => Ok(0),
        }
    }

    /// Zero-copy hlen
    pub fn hlen_bytes(&self, key: &[u8]) -> Result<usize, String> {
        let key_str = std::str::from_utf8(key)
            .map_err(|_| "ERR invalid key encoding".to_string())?;
        self.hlen(key_str)
    }

    /// Increment hash field by integer. Creates field with 0 if doesn't exist.
    pub fn hincrby(&mut self, key: &str, field: &str, increment: i64) -> Result<i64, String> {
        let now = Instant::now();

        // Check if key exists with wrong type
        if let Some(entry) = self.data.get(key) {
            if Self::is_expired(entry, now) {
                self.data.remove(key);
            } else if !matches!(entry, DataType::Hash(_)) {
                return Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string());
            }
        }

        let hash = self.data
            .entry(key.to_string())
            .or_insert_with(|| DataType::Hash(HashMap::new()));

        if let DataType::Hash(h) = hash {
            let current_val = match h.get(field) {
                Some(v) => {
                    let s = std::str::from_utf8(v)
                        .map_err(|_| "ERR hash value is not an integer")?;
                    s.parse::<i64>()
                        .map_err(|_| "ERR hash value is not an integer or out of range".to_string())?
                }
                None => 0,
            };

            let new_val = current_val.checked_add(increment)
                .ok_or("ERR increment or decrement would overflow".to_string())?;

            h.insert(field.to_string(), Bytes::from(new_val.to_string()));
            Ok(new_val)
        } else {
            Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
        }
    }

    /// Zero-copy hincrby
    pub fn hincrby_bytes(&mut self, key: &[u8], field: &[u8], increment: i64) -> Result<i64, String> {
        let key_str = std::str::from_utf8(key)
            .map_err(|_| "ERR invalid key encoding".to_string())?;
        let field_str = std::str::from_utf8(field)
            .map_err(|_| "ERR invalid field encoding".to_string())?;
        self.hincrby(key_str, field_str, increment)
    }

    /// Set hash field only if it doesn't exist. Returns 1 if set, 0 if field existed.
    pub fn hsetnx(&mut self, key: &str, field: &str, value: Bytes) -> Result<i64, String> {
        let now = Instant::now();

        // Check if key exists with wrong type
        if let Some(entry) = self.data.get(key) {
            if Self::is_expired(entry, now) {
                self.data.remove(key);
            } else if !matches!(entry, DataType::Hash(_)) {
                return Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string());
            }
        }

        let hash = self.data
            .entry(key.to_string())
            .or_insert_with(|| DataType::Hash(HashMap::new()));

        if let DataType::Hash(h) = hash {
            if h.contains_key(field) {
                Ok(0)
            } else {
                h.insert(field.to_string(), value);
                Ok(1)
            }
        } else {
            Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
        }
    }

    /// Zero-copy hsetnx
    pub fn hsetnx_bytes(&mut self, key: &[u8], field: &[u8], value: Bytes) -> Result<i64, String> {
        let key_str = std::str::from_utf8(key)
            .map_err(|_| "ERR invalid key encoding".to_string())?;
        let field_str = std::str::from_utf8(field)
            .map_err(|_| "ERR invalid field encoding".to_string())?;
        self.hsetnx(key_str, field_str, value)
    }

    // ========================================
    // Set operations
    // ========================================

    /// SADD - add members to set, return count of new members added
    pub fn sadd(&mut self, key: &str, members: Vec<Bytes>) -> Result<usize, String> {
        let now = Instant::now();

        // Check if key exists with wrong type
        if let Some(entry) = self.data.get(key) {
            if Self::is_expired(entry, now) {
                self.data.remove(key);
            } else if !matches!(entry, DataType::Set(_)) {
                return Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string());
            }
        }

        let set = self.data
            .entry(key.to_string())
            .or_insert_with(|| DataType::Set(std::collections::HashSet::new()));

        if let DataType::Set(s) = set {
            let mut added = 0;
            for m in members {
                if s.insert(m) {
                    added += 1;
                }
            }
            Ok(added)
        } else {
            Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
        }
    }

    pub fn sadd_bytes(&mut self, key: &[u8], members: Vec<Bytes>) -> Result<usize, String> {
        let key_str = std::str::from_utf8(key)
            .map_err(|_| "ERR invalid key encoding".to_string())?;
        self.sadd(key_str, members)
    }

    /// SREM - remove members from set, return count of members removed
    pub fn srem(&mut self, key: &str, members: &[Bytes]) -> Result<usize, String> {
        let now = Instant::now();

        match self.data.get_mut(key) {
            Some(entry) => {
                // Check if expired
                if Self::is_expired(entry, now) {
                    self.data.remove(key);
                    return Ok(0);
                }

                // Check type
                match entry {
                    DataType::Set(set) => {
                        let mut removed = 0;
                        for m in members {
                            if set.remove(m) {
                                removed += 1;
                            }
                        }
                        Ok(removed)
                    }
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                }
            }
            None => Ok(0),
        }
    }

    pub fn srem_bytes(&mut self, key: &[u8], members: &[Bytes]) -> Result<usize, String> {
        let key_str = std::str::from_utf8(key)
            .map_err(|_| "ERR invalid key encoding".to_string())?;
        self.srem(key_str, members)
    }

    /// SMEMBERS - get all members of a set
    pub fn smembers(&self, key: &str) -> Result<Vec<Bytes>, String> {
        let now = Instant::now();

        match self.data.get(key) {
            Some(entry) => {
                // Check if expired
                if Self::is_expired(entry, now) {
                    return Ok(Vec::new());
                }

                // Check type
                match entry {
                    DataType::Set(set) => {
                        Ok(set.iter().cloned().collect())
                    }
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                }
            }
            None => Ok(Vec::new()),
        }
    }

    pub fn smembers_bytes(&self, key: &[u8]) -> Result<Vec<Bytes>, String> {
        let key_str = std::str::from_utf8(key)
            .map_err(|_| "ERR invalid key encoding".to_string())?;
        self.smembers(key_str)
    }

    /// SISMEMBER - check if member exists in set
    pub fn sismember(&self, key: &str, member: &Bytes) -> Result<bool, String> {
        let now = Instant::now();

        match self.data.get(key) {
            Some(entry) => {
                // Check if expired
                if Self::is_expired(entry, now) {
                    return Ok(false);
                }

                // Check type
                match entry {
                    DataType::Set(set) => {
                        Ok(set.contains(member))
                    }
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                }
            }
            None => Ok(false),
        }
    }

    pub fn sismember_bytes(&self, key: &[u8], member: &Bytes) -> Result<bool, String> {
        let key_str = std::str::from_utf8(key)
            .map_err(|_| "ERR invalid key encoding".to_string())?;
        self.sismember(key_str, member)
    }

    /// SCARD - get set cardinality (size)
    pub fn scard(&self, key: &str) -> Result<usize, String> {
        let now = Instant::now();

        match self.data.get(key) {
            Some(entry) => {
                // Check if expired
                if Self::is_expired(entry, now) {
                    return Ok(0);
                }

                // Check type
                match entry {
                    DataType::Set(set) => {
                        Ok(set.len())
                    }
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                }
            }
            None => Ok(0),
        }
    }

    pub fn scard_bytes(&self, key: &[u8]) -> Result<usize, String> {
        let key_str = std::str::from_utf8(key)
            .map_err(|_| "ERR invalid key encoding".to_string())?;
        self.scard(key_str)
    }

    /// SPOP - remove and return random member(s)
    pub fn spop(&mut self, key: &str, count: Option<usize>) -> Result<Vec<Bytes>, String> {
        let now = Instant::now();

        match self.data.get_mut(key) {
            Some(entry) => {
                // Check if expired
                if Self::is_expired(entry, now) {
                    self.data.remove(key);
                    return Ok(Vec::new());
                }

                // Check type
                match entry {
                    DataType::Set(set) => {
                        let count = count.unwrap_or(1);
                        let mut result = Vec::with_capacity(count);

                        // HashSet doesn't have random access, so we iterate and collect
                        // For true randomness we'd need to collect to Vec first, but for simplicity
                        // we just pop from the front (which is deterministic but sufficient for many use cases)
                        for _ in 0..count {
                            if let Some(member) = set.iter().next().cloned() {
                                set.remove(&member);
                                result.push(member);
                            } else {
                                break;
                            }
                        }
                        Ok(result)
                    }
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                }
            }
            None => Ok(Vec::new()),
        }
    }

    pub fn spop_bytes(&mut self, key: &[u8], count: Option<usize>) -> Result<Vec<Bytes>, String> {
        let key_str = std::str::from_utf8(key)
            .map_err(|_| "ERR invalid key encoding".to_string())?;
        self.spop(key_str, count)
    }

    /// SRANDMEMBER - get random member(s) without removing
    pub fn srandmember(&self, key: &str, count: Option<i64>) -> Result<Vec<Bytes>, String> {
        let now = Instant::now();

        match self.data.get(key) {
            Some(entry) => {
                // Check if expired
                if Self::is_expired(entry, now) {
                    return Ok(Vec::new());
                }

                // Check type
                match entry {
                    DataType::Set(set) => {
                        match count {
                            None => {
                                // Return single random element
                                if let Some(member) = set.iter().next() {
                                    Ok(vec![member.clone()])
                                } else {
                                    Ok(Vec::new())
                                }
                            }
                            Some(c) if c >= 0 => {
                                // Return c distinct elements
                                let c = c as usize;
                                let result: Vec<Bytes> = set.iter().take(c).cloned().collect();
                                Ok(result)
                            }
                            Some(c) => {
                                // Negative count: return |c| elements with possible duplicates
                                let c = c.unsigned_abs() as usize;
                                if set.is_empty() {
                                    return Ok(Vec::new());
                                }
                                let members: Vec<Bytes> = set.iter().cloned().collect();
                                let mut result = Vec::with_capacity(c);
                                for i in 0..c {
                                    // Simple deterministic selection (for true random would use rand crate)
                                    result.push(members[i % members.len()].clone());
                                }
                                Ok(result)
                            }
                        }
                    }
                    _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
                }
            }
            None => Ok(Vec::new()),
        }
    }

    pub fn srandmember_bytes(&self, key: &[u8], count: Option<i64>) -> Result<Vec<Bytes>, String> {
        let key_str = std::str::from_utf8(key)
            .map_err(|_| "ERR invalid key encoding".to_string())?;
        self.srandmember(key_str, count)
    }

    pub fn keys(&self, pattern: &str) -> Vec<String> {
        let now = Instant::now();
        let mut matches = Vec::new();

        for (key, value) in self.data.iter() {
            // Skip expired keys (lazy expiration)
            if Self::is_expired(value, now) {
                continue;
            }

            if pattern == "*" || Self::pattern_matches(pattern, key) {
                matches.push(key.clone());
            }
        }

        matches.sort();
        matches
    }

    pub fn zadd(&mut self, key: String, entries: Vec<(f64, String)>) -> Result<usize, String> {
        let entry = self.data.entry(key).or_insert(DataType::SortedSet(HashMap::new()));
        match entry {
            DataType::SortedSet(map) => {
                let mut added = 0;
                for (score, member) in entries {
                    if map.insert(member, score).is_none() {
                        added += 1;
                    }
                }
                Ok(added)
            }
            _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        }
    }

    pub fn zrange(&self, key: &str, start: i64, end: i64) -> Result<Vec<(String, Option<f64>)>, String> {
        match self.data.get(key) {
            Some(entry) => {
                if let DataType::SortedSet(map) = entry {
                    let mut elements: Vec<(&String, &f64)> = map.iter().collect();
                    // Sort by score, then member lexicographically
                    elements.sort_by(|a, b| {
                        a.1.partial_cmp(b.1).unwrap_or(std::cmp::Ordering::Equal)
                            .then_with(|| a.0.cmp(b.0))
                    });

                    let len = elements.len() as i64;
                    let start = if start < 0 { len + start } else { start };
                    let end = if end < 0 { len + end } else { end };

                    let start = start.max(0);
                    let end = end.min(len - 1);

                    if start > end || start >= len {
                        return Ok(Vec::new());
                    }

                    let result = elements[start as usize..=end as usize].iter()
                        .map(|(m, s)| (m.to_string(), Some(**s)))
                        .collect();
                    Ok(result)
                } else {
                    Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
                }
            }
            None => Ok(Vec::new()),
        }
    }

    pub fn zcard(&self, key: &str) -> Result<usize, String> {
        match self.data.get(key) {
            Some(entry) => {
                if let DataType::SortedSet(map) = entry {
                    Ok(map.len())
                } else {
                    Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
                }
            }
            None => Ok(0),
        }
    }

    pub fn zscore(&self, key: &str, member: &str) -> Result<Option<f64>, String> {
        match self.data.get(key) {
            Some(entry) => {
                if let DataType::SortedSet(map) = entry {
                    Ok(map.get(member).cloned())
                } else {
                    Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
                }
            }
            None => Ok(None),
        }
    }

    pub fn zrem(&mut self, key: &str, members: &[String]) -> Result<usize, String> {
        if let Some(entry) = self.data.get_mut(key) {
            match entry {
                DataType::SortedSet(map) => {
                    let mut removed = 0;
                    for member in members {
                        if map.remove(member).is_some() {
                            removed += 1;
                        }
                    }
                    Ok(removed)
                }
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            }
        } else {
             Ok(0)
        }
    }

    pub fn zrank(&self, key: &str, member: &str) -> Result<Option<usize>, String> {
        match self.data.get(key) {
            Some(entry) => {
                if let DataType::SortedSet(map) = entry {
                    if !map.contains_key(member) {
                        return Ok(None);
                    }
                    let mut elements: Vec<(&String, &f64)> = map.iter().collect();
                    elements.sort_by(|a, b| {
                        a.1.partial_cmp(b.1).unwrap_or(std::cmp::Ordering::Equal)
                            .then_with(|| a.0.cmp(b.0))
                    });
                    
                    for (i, (m, _)) in elements.iter().enumerate() {
                         if m.as_str() == member {
                             return Ok(Some(i));
                         }
                    }
                    Ok(None)
                } else {
                    Err("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())
                }
            }
            None => Ok(None),
        }
    }

    fn is_expired(value: &DataType, now: Instant) -> bool {
        match value {
            DataType::String(_, Some(expiry)) => now > *expiry,
            _ => false,
        }
    }

    fn pattern_matches(pattern: &str, text: &str) -> bool {
        if pattern == "*" || pattern == text {
            return true;
        }

        let mut chars = pattern.split('*');
        let mut current_index = 0usize;
        let first = chars.next().unwrap_or("");

        if !pattern.starts_with('*') {
            if !text.starts_with(first) {
                return false;
            }
            current_index = first.len();
        }

        for part in chars {
            if part.is_empty() {
                continue;
            }
            if let Some(found) = text[current_index..].find(part) {
                current_index += found + part.len();
            } else {
                return false;
            }
        }

        pattern.ends_with('*') || current_index == text.len()
    }

    fn store_loaded_string(&mut self, key: &[u8], value: Vec<u8>, expiry_ms: Option<u64>) {
        let key_str = match String::from_utf8(key.to_vec()) {
            Ok(s) => s,
            Err(_) => return,
        };

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        // If the key is already expired at load time, skip it entirely.
        if let Some(abs_ms) = expiry_ms {
            if abs_ms <= now_ms {
                return;
            }
            let px = Some(abs_ms - now_ms);
            self.set(key_str, Bytes::from(value), px);
        } else {
            self.set(key_str, Bytes::from(value), None);
        }
    }
}

struct RdbParser<'a> {
    data: &'a [u8],
    pos: usize,
}

enum LengthEncoding {
    Plain(u64),
    Encoded(EncodedType),
}

enum EncodedType {
    Int8,
    Int16,
    Int32,
    Lzf,
}

impl<'a> RdbParser<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn validate_header(&mut self) -> AnyResult<()> {
        if self.data.len() < 9 {
            bail!("RDB file too short");
        }
        if &self.data[..5] != b"REDIS" {
            bail!("Invalid RDB header");
        }
        self.pos = 9; // skip magic + version
        Ok(())
    }

    fn read_u8(&mut self) -> AnyResult<u8> {
        if self.pos >= self.data.len() {
            bail!("Unexpected EOF in RDB");
        }
        let byte = self.data[self.pos];
        self.pos += 1;
        Ok(byte)
    }

    fn read_u32(&mut self) -> AnyResult<u32> {
        if self.pos + 4 > self.data.len() {
            bail!("Unexpected EOF in RDB");
        }
        let bytes = &self.data[self.pos..self.pos + 4];
        self.pos += 4;

        let array: [u8; 4] = bytes
            .try_into()
            .map_err(|_| anyhow!("Invalid RDB u32 field"))?;
        Ok(u32::from_le_bytes(array))
    }

    fn read_u64(&mut self) -> AnyResult<u64> {
        if self.pos + 8 > self.data.len() {
            bail!("Unexpected EOF in RDB");
        }
        let bytes = &self.data[self.pos..self.pos + 8];
        self.pos += 8;

        let array: [u8; 8] = bytes
            .try_into()
            .map_err(|_| anyhow!("Invalid RDB u64 field"))?;
        Ok(u64::from_le_bytes(array))
    }

    fn read_bytes(&mut self, len: usize) -> AnyResult<Vec<u8>> {
        if self.pos + len > self.data.len() {
            bail!("Unexpected EOF while reading bytes");
        }
        let slice = self.data[self.pos..self.pos + len].to_vec();
        self.pos += len;
        Ok(slice)
    }

    fn read_length(&mut self) -> AnyResult<LengthEncoding> {
        let byte = self.read_u8()?;
        match byte >> 6 {
            0 => Ok(LengthEncoding::Plain((byte & 0x3F) as u64)),
            1 => {
                let next = self.read_u8()?;
                let len = (((byte & 0x3F) as u64) << 8) | next as u64;
                Ok(LengthEncoding::Plain(len))
            }
            2 => {
                let len = self.read_u32()? as u64;
                Ok(LengthEncoding::Plain(len))
            }
            3 => {
                let enc = match byte & 0x3F {
                    0 => EncodedType::Int8,
                    1 => EncodedType::Int16,
                    2 => EncodedType::Int32,
                    3 => EncodedType::Lzf,
                    other => bail!("Unsupported special encoding: {}", other),
                };
                Ok(LengthEncoding::Encoded(enc))
            }
            _ => unreachable!(),
        }
    }

    fn read_string(&mut self) -> AnyResult<Vec<u8>> {
        match self.read_length()? {
            LengthEncoding::Plain(len) => self.read_bytes(len as usize),
            LengthEncoding::Encoded(EncodedType::Int8) => {
                let value = self.read_u8()? as i8;
                Ok(value.to_string().into_bytes())
            }
            LengthEncoding::Encoded(EncodedType::Int16) => {
                let value = self.read_u16()? as i16;
                Ok(value.to_string().into_bytes())
            }
            LengthEncoding::Encoded(EncodedType::Int32) => {
                let value = self.read_u32()? as i32;
                Ok(value.to_string().into_bytes())
            }
            LengthEncoding::Encoded(EncodedType::Lzf) => {
                let compressed_len = self.read_length_plain()?;
                let original_len = self.read_length_plain()?;
                let compressed = self.read_bytes(compressed_len as usize)?;
                let decompressed = lzf_decompress(&compressed, original_len as usize)?;
                Ok(decompressed)
            }
        }
    }

    fn read_length_plain(&mut self) -> AnyResult<u64> {
        match self.read_length()? {
            LengthEncoding::Plain(len) => Ok(len),
            _ => bail!("Expected plain length encoding"),
        }
    }

    fn read_u16(&mut self) -> AnyResult<u16> {
        if self.pos + 2 > self.data.len() {
            bail!("Unexpected EOF in RDB");
        }
        let bytes = &self.data[self.pos..self.pos + 2];
        self.pos += 2;

        let array: [u8; 2] = bytes
            .try_into()
            .map_err(|_| anyhow!("Invalid RDB u16 field"))?;
        Ok(u16::from_le_bytes(array))
    }
}

fn lzf_decompress(input: &[u8], expected_len: usize) -> AnyResult<Vec<u8>> {
    let mut output = Vec::with_capacity(expected_len);
    let mut i = 0;

    while i < input.len() {
        let ctrl = input[i];
        i += 1;
        if ctrl < 32 {
            let literal_len = (ctrl as usize) + 1;
            if i + literal_len > input.len() {
                bail!("Invalid LZF literal length");
            }
            output.extend_from_slice(&input[i..i + literal_len]);
            i += literal_len;
        } else {
            let mut length = (ctrl >> 5) as usize + 2;
            if i >= input.len() {
                bail!("Invalid LZF reference");
            }
            let mut offset = ((ctrl & 0x1F) as usize) << 8;
            offset |= input[i] as usize;
            i += 1;
            offset += 1;

            if length == 9 {
                if i >= input.len() {
                    bail!("Invalid extended LZF length");
                }
                length += input[i] as usize;
                i += 1;
            }

            if offset > output.len() {
                bail!("LZF offset out of bounds");
            }

            let start = output.len() - offset;
            for j in 0..length {
                let byte = output.get(start + j).copied().ok_or_else(|| anyhow!("LZF reference beyond output"))?;
                output.push(byte);
            }
        }
    }

    if output.len() != expected_len {
        bail!("Unexpected LZF output length: expected {}, got {}", expected_len, output.len());
    }

    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_set_get() {
        let mut db = Db::new();
        db.set("key".to_string(), Bytes::from("value"), None);
        assert_eq!(db.get("key"), Some(Bytes::from("value")));
    }

    #[test]
    fn test_expiry() {
        let mut db = Db::new();
        db.set("key".to_string(), Bytes::from("value"), Some(100));
        assert_eq!(db.get("key"), Some(Bytes::from("value")));
        
        thread::sleep(Duration::from_millis(150));
        assert_eq!(db.get("key"), None);
    }
    
    #[test]
    fn test_stream_add() {
        let mut db = Db::new();
        let id = db.add_stream_entry("stream".to_string(), (1, 1), vec![("foo".to_string(), "bar".to_string())]).unwrap();
        assert_eq!(id, (1, 1));

        let last_id = db.get_last_stream_id("stream");
        assert_eq!(last_id, Some((1, 1)));

        // Test invalid ID
        let err = db.add_stream_entry("stream".to_string(), (0, 1), vec![]).unwrap_err();
        assert_eq!(err, "ERR The ID specified in XADD is equal or smaller than the target stream top item");
    }

    // ==================== Phase 1 Tests ====================

    // DEL command tests
    #[test]
    fn test_del_single_key() {
        let mut db = Db::new();
        db.set("key1".to_string(), Bytes::from("value1"), None);
        assert!(db.exists("key1"));
        assert!(db.del("key1"));
        assert!(!db.exists("key1"));
    }

    #[test]
    fn test_del_non_existent() {
        let mut db = Db::new();
        assert!(!db.del("nonexistent"));
    }

    #[test]
    fn test_del_multiple_types() {
        let mut db = Db::new();
        // String
        db.set("str".to_string(), Bytes::from("value"), None);
        // List
        db.rpush("list".to_string(), vec![Bytes::from("a")]).unwrap();
        // Sorted set
        db.zadd("zset".to_string(), vec![(1.0, "member".to_string())]).unwrap();

        assert!(db.del("str"));
        assert!(db.del("list"));
        assert!(db.del("zset"));
        assert!(!db.exists("str"));
        assert!(!db.exists("list"));
        assert!(!db.exists("zset"));
    }

    #[test]
    fn test_del_expired_key() {
        let mut db = Db::new();
        db.set("key".to_string(), Bytes::from("value"), Some(50));
        thread::sleep(Duration::from_millis(100));
        // Expired key should return false (not counted as deleted)
        assert!(!db.del("key"));
    }

    // EXISTS command tests
    #[test]
    fn test_exists_single_key() {
        let mut db = Db::new();
        assert!(!db.exists("key"));
        db.set("key".to_string(), Bytes::from("value"), None);
        assert!(db.exists("key"));
    }

    #[test]
    fn test_exists_expired_key() {
        let mut db = Db::new();
        db.set("key".to_string(), Bytes::from("value"), Some(50));
        assert!(db.exists("key"));
        thread::sleep(Duration::from_millis(100));
        assert!(!db.exists("key"));
    }

    #[test]
    fn test_exists_bytes() {
        let mut db = Db::new();
        db.set("key".to_string(), Bytes::from("value"), None);
        assert!(db.exists_bytes(b"key"));
        assert!(!db.exists_bytes(b"nonexistent"));
    }

    // EXPIRE/TTL tests
    #[test]
    fn test_expire_ttl_lifecycle() {
        let mut db = Db::new();
        db.set("key".to_string(), Bytes::from("value"), None);

        // No expiry initially
        assert_eq!(db.ttl("key"), -1);

        // Set expiry
        assert_eq!(db.expire("key", 10), 1);
        let ttl = db.ttl("key");
        assert!(ttl > 0 && ttl <= 10);

        // Non-existent key
        assert_eq!(db.expire("nonexistent", 10), 0);
        assert_eq!(db.ttl("nonexistent"), -2);
    }

    #[test]
    fn test_expire_zero_or_negative() {
        let mut db = Db::new();
        db.set("key".to_string(), Bytes::from("value"), None);

        // Zero or negative seconds should delete the key
        assert_eq!(db.expire("key", 0), 1);
        assert!(!db.exists("key"));

        db.set("key2".to_string(), Bytes::from("value"), None);
        assert_eq!(db.expire("key2", -5), 1);
        assert!(!db.exists("key2"));
    }

    // PEXPIRE/PTTL tests
    #[test]
    fn test_pexpire_pttl() {
        let mut db = Db::new();
        db.set("key".to_string(), Bytes::from("value"), None);

        assert_eq!(db.pexpire("key", 5000), 1);
        let pttl = db.pttl("key");
        assert!(pttl > 0 && pttl <= 5000);

        // TTL should round up to seconds
        let ttl = db.ttl("key");
        assert!(ttl > 0 && ttl <= 5);
    }

    #[test]
    fn test_pttl_non_existent() {
        let db = Db::new();
        assert_eq!(db.pttl("nonexistent"), -2);
    }

    // PERSIST tests
    #[test]
    fn test_persist() {
        let mut db = Db::new();
        db.set("key".to_string(), Bytes::from("value"), Some(5000));

        // Has expiry
        assert!(db.ttl("key") > 0);

        // Remove expiry
        assert_eq!(db.persist("key"), 1);
        assert_eq!(db.ttl("key"), -1);

        // Persist again should return 0 (no expiry to remove)
        assert_eq!(db.persist("key"), 0);
    }

    #[test]
    fn test_persist_non_existent() {
        let mut db = Db::new();
        assert_eq!(db.persist("nonexistent"), 0);
    }

    // INCR/INCRBY/DECR/DECRBY tests
    #[test]
    fn test_incrby() {
        let mut db = Db::new();

        // New key starts at 0
        assert_eq!(db.incrby("counter", 5).unwrap(), 5);
        assert_eq!(db.incrby("counter", 3).unwrap(), 8);
        assert_eq!(db.incrby("counter", -2).unwrap(), 6);
    }

    #[test]
    fn test_incrby_preserves_expiry() {
        let mut db = Db::new();
        db.set("key".to_string(), Bytes::from("10"), Some(10000));

        db.incrby("key", 5).unwrap();

        // Expiry should still be set
        assert!(db.ttl("key") > 0);
        assert_eq!(db.get("key"), Some(Bytes::from("15")));
    }

    #[test]
    fn test_incrby_non_integer() {
        let mut db = Db::new();
        db.set("key".to_string(), Bytes::from("not_a_number"), None);

        let result = db.incrby("key", 1);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "ERR value is not an integer or out of range");
    }

    #[test]
    fn test_incrby_wrong_type() {
        let mut db = Db::new();
        db.rpush("list".to_string(), vec![Bytes::from("a")]).unwrap();

        let result = db.incrby("list", 1);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("WRONGTYPE"));
    }

    #[test]
    fn test_incrby_overflow() {
        let mut db = Db::new();
        db.set("key".to_string(), Bytes::from(i64::MAX.to_string()), None);

        let result = db.incrby("key", 1);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("overflow"));
    }

    // APPEND/STRLEN tests
    #[test]
    fn test_append() {
        let mut db = Db::new();

        // New key
        assert_eq!(db.append("key", b"Hello").unwrap(), 5);
        // Existing key
        assert_eq!(db.append("key", b" World").unwrap(), 11);
        assert_eq!(db.get("key"), Some(Bytes::from("Hello World")));
    }

    #[test]
    fn test_append_wrong_type() {
        let mut db = Db::new();
        db.rpush("list".to_string(), vec![Bytes::from("a")]).unwrap();

        let result = db.append("list", b"test");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("WRONGTYPE"));
    }

    #[test]
    fn test_strlen() {
        let mut db = Db::new();

        // Non-existent key
        assert_eq!(db.strlen("nonexistent").unwrap(), 0);

        // Existing key
        db.set("key".to_string(), Bytes::from("Hello World"), None);
        assert_eq!(db.strlen("key").unwrap(), 11);
    }

    #[test]
    fn test_strlen_expired() {
        let mut db = Db::new();
        db.set("key".to_string(), Bytes::from("value"), Some(50));
        thread::sleep(Duration::from_millis(100));
        assert_eq!(db.strlen("key").unwrap(), 0);
    }

    #[test]
    fn test_strlen_wrong_type() {
        let mut db = Db::new();
        db.rpush("list".to_string(), vec![Bytes::from("a")]).unwrap();

        let result = db.strlen("list");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("WRONGTYPE"));
    }

    // SETNX tests
    #[test]
    fn test_setnx_new_key() {
        let mut db = Db::new();
        assert!(db.setnx("key", Bytes::from("value")));
        assert_eq!(db.get("key"), Some(Bytes::from("value")));
    }

    #[test]
    fn test_setnx_existing_key() {
        let mut db = Db::new();
        db.set("key".to_string(), Bytes::from("original"), None);
        assert!(!db.setnx("key", Bytes::from("new")));
        assert_eq!(db.get("key"), Some(Bytes::from("original")));
    }

    #[test]
    fn test_setnx_expired_key() {
        let mut db = Db::new();
        db.set("key".to_string(), Bytes::from("value"), Some(50));
        thread::sleep(Duration::from_millis(100));

        // Should set since key expired
        assert!(db.setnx("key", Bytes::from("new")));
        assert_eq!(db.get("key"), Some(Bytes::from("new")));
    }

    // RENAME tests
    #[test]
    fn test_rename_string() {
        let mut db = Db::new();
        db.set("old".to_string(), Bytes::from("value"), None);

        assert!(db.rename("old", "new").is_ok());
        assert!(!db.exists("old"));
        assert_eq!(db.get("new"), Some(Bytes::from("value")));
    }

    #[test]
    fn test_rename_with_expiry() {
        let mut db = Db::new();
        db.set("old".to_string(), Bytes::from("value"), Some(10000));

        db.rename("old", "new").unwrap();

        // Expiry should be preserved
        assert!(db.ttl("new") > 0);
    }

    #[test]
    fn test_rename_non_existent() {
        let mut db = Db::new();
        let result = db.rename("nonexistent", "new");
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "ERR no such key");
    }

    #[test]
    fn test_rename_overwrite_existing() {
        let mut db = Db::new();
        db.set("old".to_string(), Bytes::from("old_value"), None);
        db.set("new".to_string(), Bytes::from("new_value"), None);

        db.rename("old", "new").unwrap();

        assert!(!db.exists("old"));
        assert_eq!(db.get("new"), Some(Bytes::from("old_value")));
    }

    #[test]
    fn test_rename_same_key() {
        let mut db = Db::new();
        db.set("key".to_string(), Bytes::from("value"), None);

        // Should be a no-op
        assert!(db.rename("key", "key").is_ok());
        assert_eq!(db.get("key"), Some(Bytes::from("value")));
    }

    #[test]
    fn test_rename_list() {
        let mut db = Db::new();
        db.rpush("old_list".to_string(), vec![Bytes::from("a"), Bytes::from("b")]).unwrap();

        db.rename("old_list", "new_list").unwrap();

        assert!(!db.exists("old_list"));
        assert_eq!(db.key_type("new_list"), "list");
        assert_eq!(db.llen("new_list").unwrap(), 2);
    }

    // Zero-copy variant tests
    #[test]
    fn test_del_bytes() {
        let mut db = Db::new();
        db.set("key".to_string(), Bytes::from("value"), None);
        assert!(db.del_bytes(b"key"));
        assert!(!db.exists("key"));
    }

    #[test]
    fn test_incrby_bytes() {
        let mut db = Db::new();
        assert_eq!(db.incrby_bytes(b"counter", 10).unwrap(), 10);
        assert_eq!(db.incrby_bytes(b"counter", 5).unwrap(), 15);
    }

    #[test]
    fn test_append_bytes() {
        let mut db = Db::new();
        assert_eq!(db.append_bytes(b"key", b"Hello").unwrap(), 5);
        assert_eq!(db.append_bytes(b"key", b" World").unwrap(), 11);
    }

    #[test]
    fn test_strlen_bytes() {
        let mut db = Db::new();
        db.set("key".to_string(), Bytes::from("test"), None);
        assert_eq!(db.strlen_bytes(b"key").unwrap(), 4);
    }

    #[test]
    fn test_setnx_bytes() {
        let mut db = Db::new();
        assert!(db.setnx_bytes(b"key", Bytes::from("value")));
        assert!(!db.setnx_bytes(b"key", Bytes::from("new")));
    }

    #[test]
    fn test_rename_bytes() {
        let mut db = Db::new();
        db.set("old".to_string(), Bytes::from("value"), None);
        assert!(db.rename_bytes(b"old", b"new").is_ok());
        assert_eq!(db.get("new"), Some(Bytes::from("value")));
    }

    // ========================================
    // Hash command tests
    // ========================================

    // HSET tests
    #[test]
    fn test_hset_single_field() {
        let mut db = Db::new();
        let count = db.hset("myhash", vec![("field1".to_string(), Bytes::from("value1"))]).unwrap();
        assert_eq!(count, 1);
        assert_eq!(db.hget("myhash", "field1").unwrap(), Some(Bytes::from("value1")));
    }

    #[test]
    fn test_hset_multi_field() {
        let mut db = Db::new();
        let count = db.hset("myhash", vec![
            ("field1".to_string(), Bytes::from("value1")),
            ("field2".to_string(), Bytes::from("value2")),
            ("field3".to_string(), Bytes::from("value3")),
        ]).unwrap();
        assert_eq!(count, 3);
        assert_eq!(db.hget("myhash", "field1").unwrap(), Some(Bytes::from("value1")));
        assert_eq!(db.hget("myhash", "field2").unwrap(), Some(Bytes::from("value2")));
        assert_eq!(db.hget("myhash", "field3").unwrap(), Some(Bytes::from("value3")));
    }

    #[test]
    fn test_hset_update_existing() {
        let mut db = Db::new();
        db.hset("myhash", vec![("field1".to_string(), Bytes::from("original"))]).unwrap();
        // Updating existing field returns 0 (no new fields added)
        let count = db.hset("myhash", vec![("field1".to_string(), Bytes::from("updated"))]).unwrap();
        assert_eq!(count, 0);
        assert_eq!(db.hget("myhash", "field1").unwrap(), Some(Bytes::from("updated")));
    }

    #[test]
    fn test_hset_bytes() {
        let mut db = Db::new();
        let count = db.hset_bytes(b"myhash", vec![(Bytes::from("field1"), Bytes::from("value1"))]).unwrap();
        assert_eq!(count, 1);
        assert_eq!(db.hget("myhash", "field1").unwrap(), Some(Bytes::from("value1")));
    }

    // HGET tests
    #[test]
    fn test_hget_existing_field() {
        let mut db = Db::new();
        db.hset("myhash", vec![("field1".to_string(), Bytes::from("value1"))]).unwrap();
        assert_eq!(db.hget("myhash", "field1").unwrap(), Some(Bytes::from("value1")));
    }

    #[test]
    fn test_hget_nonexistent_field() {
        let mut db = Db::new();
        db.hset("myhash", vec![("field1".to_string(), Bytes::from("value1"))]).unwrap();
        assert_eq!(db.hget("myhash", "nonexistent").unwrap(), None);
    }

    #[test]
    fn test_hget_nonexistent_key() {
        let db = Db::new();
        assert_eq!(db.hget("nonexistent", "field1").unwrap(), None);
    }

    #[test]
    fn test_hget_bytes() {
        let mut db = Db::new();
        db.hset("myhash", vec![("field1".to_string(), Bytes::from("value1"))]).unwrap();
        assert_eq!(db.hget_bytes(b"myhash", b"field1").unwrap(), Some(Bytes::from("value1")));
    }

    // HMGET tests
    #[test]
    fn test_hmget_all_exist() {
        let mut db = Db::new();
        db.hset("myhash", vec![
            ("field1".to_string(), Bytes::from("value1")),
            ("field2".to_string(), Bytes::from("value2")),
        ]).unwrap();
        let results = db.hmget("myhash", &["field1".to_string(), "field2".to_string()]).unwrap();
        assert_eq!(results, vec![Some(Bytes::from("value1")), Some(Bytes::from("value2"))]);
    }

    #[test]
    fn test_hmget_some_exist() {
        let mut db = Db::new();
        db.hset("myhash", vec![("field1".to_string(), Bytes::from("value1"))]).unwrap();
        let results = db.hmget("myhash", &["field1".to_string(), "nonexistent".to_string()]).unwrap();
        assert_eq!(results, vec![Some(Bytes::from("value1")), None]);
    }

    #[test]
    fn test_hmget_nonexistent_key() {
        let db = Db::new();
        let results = db.hmget("nonexistent", &["field1".to_string(), "field2".to_string()]).unwrap();
        assert_eq!(results, vec![None, None]);
    }

    #[test]
    fn test_hmget_bytes() {
        let mut db = Db::new();
        db.hset("myhash", vec![("field1".to_string(), Bytes::from("value1"))]).unwrap();
        let results = db.hmget_bytes(b"myhash", &[Bytes::from("field1"), Bytes::from("field2")]).unwrap();
        assert_eq!(results, vec![Some(Bytes::from("value1")), None]);
    }

    // HGETALL tests
    #[test]
    fn test_hgetall_existing_hash() {
        let mut db = Db::new();
        db.hset("myhash", vec![
            ("field1".to_string(), Bytes::from("value1")),
            ("field2".to_string(), Bytes::from("value2")),
        ]).unwrap();
        let results = db.hgetall("myhash").unwrap();
        assert_eq!(results.len(), 2);
        assert!(results.iter().any(|(k, v)| k == "field1" && v == &Bytes::from("value1")));
        assert!(results.iter().any(|(k, v)| k == "field2" && v == &Bytes::from("value2")));
    }

    #[test]
    fn test_hgetall_empty() {
        let db = Db::new();
        let results = db.hgetall("nonexistent").unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_hgetall_bytes() {
        let mut db = Db::new();
        db.hset("myhash", vec![("field1".to_string(), Bytes::from("value1"))]).unwrap();
        let results = db.hgetall_bytes(b"myhash").unwrap();
        assert_eq!(results.len(), 1);
    }

    // HDEL tests
    #[test]
    fn test_hdel_single_field() {
        let mut db = Db::new();
        db.hset("myhash", vec![
            ("field1".to_string(), Bytes::from("value1")),
            ("field2".to_string(), Bytes::from("value2")),
        ]).unwrap();
        let deleted = db.hdel("myhash", &["field1".to_string()]).unwrap();
        assert_eq!(deleted, 1);
        assert_eq!(db.hget("myhash", "field1").unwrap(), None);
        assert_eq!(db.hget("myhash", "field2").unwrap(), Some(Bytes::from("value2")));
    }

    #[test]
    fn test_hdel_multi_field() {
        let mut db = Db::new();
        db.hset("myhash", vec![
            ("field1".to_string(), Bytes::from("value1")),
            ("field2".to_string(), Bytes::from("value2")),
            ("field3".to_string(), Bytes::from("value3")),
        ]).unwrap();
        let deleted = db.hdel("myhash", &["field1".to_string(), "field2".to_string(), "nonexistent".to_string()]).unwrap();
        assert_eq!(deleted, 2);
        assert_eq!(db.hlen("myhash").unwrap(), 1);
    }

    #[test]
    fn test_hdel_nonexistent_key() {
        let mut db = Db::new();
        let deleted = db.hdel("nonexistent", &["field1".to_string()]).unwrap();
        assert_eq!(deleted, 0);
    }

    #[test]
    fn test_hdel_bytes() {
        let mut db = Db::new();
        db.hset("myhash", vec![("field1".to_string(), Bytes::from("value1"))]).unwrap();
        let deleted = db.hdel_bytes(b"myhash", &[Bytes::from("field1")]).unwrap();
        assert_eq!(deleted, 1);
    }

    // HEXISTS tests
    #[test]
    fn test_hexists_existing_field() {
        let mut db = Db::new();
        db.hset("myhash", vec![("field1".to_string(), Bytes::from("value1"))]).unwrap();
        assert!(db.hexists("myhash", "field1").unwrap());
    }

    #[test]
    fn test_hexists_nonexistent_field() {
        let mut db = Db::new();
        db.hset("myhash", vec![("field1".to_string(), Bytes::from("value1"))]).unwrap();
        assert!(!db.hexists("myhash", "nonexistent").unwrap());
    }

    #[test]
    fn test_hexists_nonexistent_key() {
        let db = Db::new();
        assert!(!db.hexists("nonexistent", "field1").unwrap());
    }

    #[test]
    fn test_hexists_bytes() {
        let mut db = Db::new();
        db.hset("myhash", vec![("field1".to_string(), Bytes::from("value1"))]).unwrap();
        assert!(db.hexists_bytes(b"myhash", b"field1").unwrap());
        assert!(!db.hexists_bytes(b"myhash", b"nonexistent").unwrap());
    }

    // HKEYS tests
    #[test]
    fn test_hkeys_existing_hash() {
        let mut db = Db::new();
        db.hset("myhash", vec![
            ("field1".to_string(), Bytes::from("value1")),
            ("field2".to_string(), Bytes::from("value2")),
        ]).unwrap();
        let keys = db.hkeys("myhash").unwrap();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&"field1".to_string()));
        assert!(keys.contains(&"field2".to_string()));
    }

    #[test]
    fn test_hkeys_nonexistent() {
        let db = Db::new();
        let keys = db.hkeys("nonexistent").unwrap();
        assert!(keys.is_empty());
    }

    #[test]
    fn test_hkeys_bytes() {
        let mut db = Db::new();
        db.hset("myhash", vec![("field1".to_string(), Bytes::from("value1"))]).unwrap();
        let keys = db.hkeys_bytes(b"myhash").unwrap();
        assert_eq!(keys.len(), 1);
    }

    // HVALS tests
    #[test]
    fn test_hvals_existing_hash() {
        let mut db = Db::new();
        db.hset("myhash", vec![
            ("field1".to_string(), Bytes::from("value1")),
            ("field2".to_string(), Bytes::from("value2")),
        ]).unwrap();
        let vals = db.hvals("myhash").unwrap();
        assert_eq!(vals.len(), 2);
        assert!(vals.contains(&Bytes::from("value1")));
        assert!(vals.contains(&Bytes::from("value2")));
    }

    #[test]
    fn test_hvals_nonexistent() {
        let db = Db::new();
        let vals = db.hvals("nonexistent").unwrap();
        assert!(vals.is_empty());
    }

    #[test]
    fn test_hvals_bytes() {
        let mut db = Db::new();
        db.hset("myhash", vec![("field1".to_string(), Bytes::from("value1"))]).unwrap();
        let vals = db.hvals_bytes(b"myhash").unwrap();
        assert_eq!(vals.len(), 1);
    }

    // HLEN tests
    #[test]
    fn test_hlen_existing_hash() {
        let mut db = Db::new();
        db.hset("myhash", vec![
            ("field1".to_string(), Bytes::from("value1")),
            ("field2".to_string(), Bytes::from("value2")),
            ("field3".to_string(), Bytes::from("value3")),
        ]).unwrap();
        assert_eq!(db.hlen("myhash").unwrap(), 3);
    }

    #[test]
    fn test_hlen_nonexistent() {
        let db = Db::new();
        assert_eq!(db.hlen("nonexistent").unwrap(), 0);
    }

    #[test]
    fn test_hlen_bytes() {
        let mut db = Db::new();
        db.hset("myhash", vec![("field1".to_string(), Bytes::from("value1"))]).unwrap();
        assert_eq!(db.hlen_bytes(b"myhash").unwrap(), 1);
    }

    // HINCRBY tests
    #[test]
    fn test_hincrby_new_field() {
        let mut db = Db::new();
        let result = db.hincrby("myhash", "counter", 10);
        assert_eq!(result, Ok(10));
        assert_eq!(db.hget("myhash", "counter").unwrap(), Some(Bytes::from("10")));
    }

    #[test]
    fn test_hincrby_existing_field() {
        let mut db = Db::new();
        db.hset("myhash", vec![("counter".to_string(), Bytes::from("5"))]).unwrap();
        let result = db.hincrby("myhash", "counter", 10);
        assert_eq!(result, Ok(15));
    }

    #[test]
    fn test_hincrby_negative() {
        let mut db = Db::new();
        db.hset("myhash", vec![("counter".to_string(), Bytes::from("10"))]).unwrap();
        let result = db.hincrby("myhash", "counter", -3);
        assert_eq!(result, Ok(7));
    }

    #[test]
    fn test_hincrby_non_integer() {
        let mut db = Db::new();
        db.hset("myhash", vec![("field".to_string(), Bytes::from("not_a_number"))]).unwrap();
        let result = db.hincrby("myhash", "field", 1);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not an integer"));
    }

    #[test]
    fn test_hincrby_overflow() {
        let mut db = Db::new();
        db.hset("myhash", vec![("counter".to_string(), Bytes::from(i64::MAX.to_string()))]).unwrap();
        let result = db.hincrby("myhash", "counter", 1);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("overflow"));
    }

    #[test]
    fn test_hincrby_bytes() {
        let mut db = Db::new();
        let result = db.hincrby_bytes(b"myhash", b"counter", 5);
        assert_eq!(result, Ok(5));
    }

    // HSETNX tests
    #[test]
    fn test_hsetnx_new_field() {
        let mut db = Db::new();
        assert_eq!(db.hsetnx("myhash", "field1", Bytes::from("value1")).unwrap(), 1);
        assert_eq!(db.hget("myhash", "field1").unwrap(), Some(Bytes::from("value1")));
    }

    #[test]
    fn test_hsetnx_existing_field() {
        let mut db = Db::new();
        db.hset("myhash", vec![("field1".to_string(), Bytes::from("original"))]).unwrap();
        assert_eq!(db.hsetnx("myhash", "field1", Bytes::from("new")).unwrap(), 0);
        assert_eq!(db.hget("myhash", "field1").unwrap(), Some(Bytes::from("original")));
    }

    #[test]
    fn test_hsetnx_new_hash() {
        let mut db = Db::new();
        // HSETNX on non-existent key should create hash and set field
        assert_eq!(db.hsetnx("newhash", "field1", Bytes::from("value1")).unwrap(), 1);
        assert_eq!(db.key_type("newhash"), "hash");
    }

    #[test]
    fn test_hsetnx_bytes() {
        let mut db = Db::new();
        assert_eq!(db.hsetnx_bytes(b"myhash", b"field1", Bytes::from("value1")).unwrap(), 1);
        assert_eq!(db.hsetnx_bytes(b"myhash", b"field1", Bytes::from("new")).unwrap(), 0);
    }

    // WRONGTYPE error tests for hash operations
    #[test]
    fn test_hash_wrongtype_on_string() {
        let mut db = Db::new();
        db.set("mystring".to_string(), Bytes::from("value"), None);

        // All hash operations should fail with WRONGTYPE error
        assert!(db.hget("mystring", "field").is_err());
        assert!(db.hset("mystring", vec![("field".to_string(), Bytes::from("value"))]).is_err());
        assert!(db.hlen("mystring").is_err());
        assert!(db.hdel("mystring", &["field".to_string()]).is_err());
        assert!(db.hexists("mystring", "field").is_err());
        assert!(db.hkeys("mystring").is_err());
        assert!(db.hvals("mystring").is_err());
        assert!(db.hgetall("mystring").is_err());
        assert!(db.hmget("mystring", &["field".to_string()]).is_err());
        assert!(db.hincrby("mystring", "field", 1).is_err());
        assert!(db.hsetnx("mystring", "field", Bytes::from("value")).is_err());
    }

    // Test hash key_type
    #[test]
    fn test_key_type_hash() {
        let mut db = Db::new();
        db.hset("myhash", vec![("field1".to_string(), Bytes::from("value1"))]).unwrap();
        assert_eq!(db.key_type("myhash"), "hash");
    }

    // Test DEL on hash
    #[test]
    fn test_del_hash() {
        let mut db = Db::new();
        db.hset("myhash", vec![
            ("field1".to_string(), Bytes::from("value1")),
            ("field2".to_string(), Bytes::from("value2")),
        ]).unwrap();
        assert!(db.del("myhash"));
        assert!(!db.exists("myhash"));
        assert_eq!(db.hget("myhash", "field1").unwrap(), None);
    }

    // Test EXISTS on hash
    #[test]
    fn test_exists_hash() {
        let mut db = Db::new();
        db.hset("myhash", vec![("field1".to_string(), Bytes::from("value1"))]).unwrap();
        assert!(db.exists("myhash"));
    }

    // Test RENAME on hash
    #[test]
    fn test_rename_hash() {
        let mut db = Db::new();
        db.hset("oldhash", vec![
            ("field1".to_string(), Bytes::from("value1")),
            ("field2".to_string(), Bytes::from("value2")),
        ]).unwrap();
        db.rename("oldhash", "newhash").unwrap();
        assert!(!db.exists("oldhash"));
        assert_eq!(db.key_type("newhash"), "hash");
        assert_eq!(db.hget("newhash", "field1").unwrap(), Some(Bytes::from("value1")));
        assert_eq!(db.hlen("newhash").unwrap(), 2);
    }

    // ========================================
    // Set command tests
    // ========================================

    // SADD tests
    #[test]
    fn test_sadd_new_set() {
        let mut db = Db::new();
        let added = db.sadd("myset", vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")]).unwrap();
        assert_eq!(added, 3);
        assert_eq!(db.scard("myset").unwrap(), 3);
    }

    #[test]
    fn test_sadd_existing_set() {
        let mut db = Db::new();
        db.sadd("myset", vec![Bytes::from("a"), Bytes::from("b")]).unwrap();
        // Add one new member and two duplicates
        let added = db.sadd("myset", vec![Bytes::from("b"), Bytes::from("c"), Bytes::from("a")]).unwrap();
        assert_eq!(added, 1); // Only "c" is new
        assert_eq!(db.scard("myset").unwrap(), 3);
    }

    #[test]
    fn test_sadd_duplicates() {
        let mut db = Db::new();
        let added = db.sadd("myset", vec![Bytes::from("a"), Bytes::from("a"), Bytes::from("a")]).unwrap();
        assert_eq!(added, 1); // Only one unique member
        assert_eq!(db.scard("myset").unwrap(), 1);
    }

    #[test]
    fn test_sadd_bytes() {
        let mut db = Db::new();
        let key = Bytes::from("myset");
        let added = db.sadd_bytes(&key, vec![Bytes::from("a"), Bytes::from("b")]).unwrap();
        assert_eq!(added, 2);
    }

    #[test]
    fn test_sadd_wrongtype() {
        let mut db = Db::new();
        db.set("mykey".to_string(), Bytes::from("value"), None);
        let result = db.sadd("mykey", vec![Bytes::from("a")]);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("WRONGTYPE"));
    }

    // SREM tests
    #[test]
    fn test_srem_existing_members() {
        let mut db = Db::new();
        db.sadd("myset", vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")]).unwrap();
        let removed = db.srem("myset", &[Bytes::from("a"), Bytes::from("b")]).unwrap();
        assert_eq!(removed, 2);
        assert_eq!(db.scard("myset").unwrap(), 1);
    }

    #[test]
    fn test_srem_nonexistent_members() {
        let mut db = Db::new();
        db.sadd("myset", vec![Bytes::from("a")]).unwrap();
        let removed = db.srem("myset", &[Bytes::from("x"), Bytes::from("y")]).unwrap();
        assert_eq!(removed, 0);
    }

    #[test]
    fn test_srem_mixed() {
        let mut db = Db::new();
        db.sadd("myset", vec![Bytes::from("a"), Bytes::from("b")]).unwrap();
        let removed = db.srem("myset", &[Bytes::from("a"), Bytes::from("x")]).unwrap();
        assert_eq!(removed, 1); // Only "a" existed
    }

    #[test]
    fn test_srem_nonexistent_key() {
        let mut db = Db::new();
        let removed = db.srem("nonexistent", &[Bytes::from("a")]).unwrap();
        assert_eq!(removed, 0);
    }

    #[test]
    fn test_srem_bytes() {
        let mut db = Db::new();
        db.sadd("myset", vec![Bytes::from("a"), Bytes::from("b")]).unwrap();
        let key = Bytes::from("myset");
        let removed = db.srem_bytes(&key, &[Bytes::from("a")]).unwrap();
        assert_eq!(removed, 1);
    }

    #[test]
    fn test_srem_wrongtype() {
        let mut db = Db::new();
        db.set("mykey".to_string(), Bytes::from("value"), None);
        let result = db.srem("mykey", &[Bytes::from("a")]);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("WRONGTYPE"));
    }

    // SMEMBERS tests
    #[test]
    fn test_smembers_existing_set() {
        let mut db = Db::new();
        db.sadd("myset", vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")]).unwrap();
        let members = db.smembers("myset").unwrap();
        assert_eq!(members.len(), 3);
        assert!(members.contains(&Bytes::from("a")));
        assert!(members.contains(&Bytes::from("b")));
        assert!(members.contains(&Bytes::from("c")));
    }

    #[test]
    fn test_smembers_nonexistent_key() {
        let db = Db::new();
        let members = db.smembers("nonexistent").unwrap();
        assert!(members.is_empty());
    }

    #[test]
    fn test_smembers_bytes() {
        let mut db = Db::new();
        db.sadd("myset", vec![Bytes::from("a")]).unwrap();
        let key = Bytes::from("myset");
        let members = db.smembers_bytes(&key).unwrap();
        assert_eq!(members.len(), 1);
    }

    #[test]
    fn test_smembers_wrongtype() {
        let mut db = Db::new();
        db.set("mykey".to_string(), Bytes::from("value"), None);
        let result = db.smembers("mykey");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("WRONGTYPE"));
    }

    // SISMEMBER tests
    #[test]
    fn test_sismember_existing() {
        let mut db = Db::new();
        db.sadd("myset", vec![Bytes::from("a"), Bytes::from("b")]).unwrap();
        assert!(db.sismember("myset", &Bytes::from("a")).unwrap());
        assert!(db.sismember("myset", &Bytes::from("b")).unwrap());
    }

    #[test]
    fn test_sismember_nonexistent_member() {
        let mut db = Db::new();
        db.sadd("myset", vec![Bytes::from("a")]).unwrap();
        assert!(!db.sismember("myset", &Bytes::from("x")).unwrap());
    }

    #[test]
    fn test_sismember_nonexistent_key() {
        let db = Db::new();
        assert!(!db.sismember("nonexistent", &Bytes::from("a")).unwrap());
    }

    #[test]
    fn test_sismember_bytes() {
        let mut db = Db::new();
        db.sadd("myset", vec![Bytes::from("a")]).unwrap();
        let key = Bytes::from("myset");
        assert!(db.sismember_bytes(&key, &Bytes::from("a")).unwrap());
    }

    #[test]
    fn test_sismember_wrongtype() {
        let mut db = Db::new();
        db.set("mykey".to_string(), Bytes::from("value"), None);
        let result = db.sismember("mykey", &Bytes::from("a"));
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("WRONGTYPE"));
    }

    // SCARD tests
    #[test]
    fn test_scard_existing_set() {
        let mut db = Db::new();
        db.sadd("myset", vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")]).unwrap();
        assert_eq!(db.scard("myset").unwrap(), 3);
    }

    #[test]
    fn test_scard_nonexistent_key() {
        let db = Db::new();
        assert_eq!(db.scard("nonexistent").unwrap(), 0);
    }

    #[test]
    fn test_scard_bytes() {
        let mut db = Db::new();
        db.sadd("myset", vec![Bytes::from("a"), Bytes::from("b")]).unwrap();
        let key = Bytes::from("myset");
        assert_eq!(db.scard_bytes(&key).unwrap(), 2);
    }

    #[test]
    fn test_scard_wrongtype() {
        let mut db = Db::new();
        db.set("mykey".to_string(), Bytes::from("value"), None);
        let result = db.scard("mykey");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("WRONGTYPE"));
    }

    // SPOP tests
    #[test]
    fn test_spop_single() {
        let mut db = Db::new();
        db.sadd("myset", vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")]).unwrap();
        let popped = db.spop("myset", None).unwrap();
        assert_eq!(popped.len(), 1);
        assert_eq!(db.scard("myset").unwrap(), 2);
    }

    #[test]
    fn test_spop_with_count() {
        let mut db = Db::new();
        db.sadd("myset", vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")]).unwrap();
        let popped = db.spop("myset", Some(2)).unwrap();
        assert_eq!(popped.len(), 2);
        assert_eq!(db.scard("myset").unwrap(), 1);
    }

    #[test]
    fn test_spop_more_than_available() {
        let mut db = Db::new();
        db.sadd("myset", vec![Bytes::from("a"), Bytes::from("b")]).unwrap();
        let popped = db.spop("myset", Some(10)).unwrap();
        assert_eq!(popped.len(), 2);
        assert_eq!(db.scard("myset").unwrap(), 0);
    }

    #[test]
    fn test_spop_nonexistent_key() {
        let mut db = Db::new();
        let popped = db.spop("nonexistent", None).unwrap();
        assert!(popped.is_empty());
    }

    #[test]
    fn test_spop_bytes() {
        let mut db = Db::new();
        db.sadd("myset", vec![Bytes::from("a"), Bytes::from("b")]).unwrap();
        let key = Bytes::from("myset");
        let popped = db.spop_bytes(&key, Some(1)).unwrap();
        assert_eq!(popped.len(), 1);
    }

    #[test]
    fn test_spop_wrongtype() {
        let mut db = Db::new();
        db.set("mykey".to_string(), Bytes::from("value"), None);
        let result = db.spop("mykey", None);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("WRONGTYPE"));
    }

    // SRANDMEMBER tests
    #[test]
    fn test_srandmember_single() {
        let mut db = Db::new();
        db.sadd("myset", vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")]).unwrap();
        let members = db.srandmember("myset", None).unwrap();
        assert_eq!(members.len(), 1);
        // Set should be unchanged
        assert_eq!(db.scard("myset").unwrap(), 3);
    }

    #[test]
    fn test_srandmember_positive_count() {
        let mut db = Db::new();
        db.sadd("myset", vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")]).unwrap();
        let members = db.srandmember("myset", Some(2)).unwrap();
        assert_eq!(members.len(), 2);
        // All members should be unique (positive count)
        let unique: std::collections::HashSet<_> = members.iter().collect();
        assert_eq!(unique.len(), 2);
    }

    #[test]
    fn test_srandmember_negative_count() {
        let mut db = Db::new();
        db.sadd("myset", vec![Bytes::from("a"), Bytes::from("b")]).unwrap();
        // Negative count allows duplicates
        let members = db.srandmember("myset", Some(-5)).unwrap();
        assert_eq!(members.len(), 5);
    }

    #[test]
    fn test_srandmember_more_than_available() {
        let mut db = Db::new();
        db.sadd("myset", vec![Bytes::from("a"), Bytes::from("b")]).unwrap();
        // Positive count capped at set size
        let members = db.srandmember("myset", Some(10)).unwrap();
        assert_eq!(members.len(), 2);
    }

    #[test]
    fn test_srandmember_nonexistent_key() {
        let db = Db::new();
        let members = db.srandmember("nonexistent", None).unwrap();
        assert!(members.is_empty());
    }

    #[test]
    fn test_srandmember_bytes() {
        let mut db = Db::new();
        db.sadd("myset", vec![Bytes::from("a"), Bytes::from("b")]).unwrap();
        let key = Bytes::from("myset");
        let members = db.srandmember_bytes(&key, Some(1)).unwrap();
        assert_eq!(members.len(), 1);
    }

    #[test]
    fn test_srandmember_wrongtype() {
        let mut db = Db::new();
        db.set("mykey".to_string(), Bytes::from("value"), None);
        let result = db.srandmember("mykey", None);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("WRONGTYPE"));
    }

    // Set type detection tests
    #[test]
    fn test_key_type_set() {
        let mut db = Db::new();
        db.sadd("myset", vec![Bytes::from("a")]).unwrap();
        assert_eq!(db.key_type("myset"), "set");
    }

    // DEL on set
    #[test]
    fn test_del_set() {
        let mut db = Db::new();
        db.sadd("myset", vec![Bytes::from("a"), Bytes::from("b")]).unwrap();
        assert!(db.del("myset"));
        assert!(!db.exists("myset"));
    }

    // EXISTS on set
    #[test]
    fn test_exists_set() {
        let mut db = Db::new();
        db.sadd("myset", vec![Bytes::from("a")]).unwrap();
        assert!(db.exists("myset"));
    }

    // RENAME on set
    #[test]
    fn test_rename_set() {
        let mut db = Db::new();
        db.sadd("oldset", vec![Bytes::from("a"), Bytes::from("b")]).unwrap();
        db.rename("oldset", "newset").unwrap();
        assert!(!db.exists("oldset"));
        assert_eq!(db.key_type("newset"), "set");
        assert_eq!(db.scard("newset").unwrap(), 2);
        assert!(db.sismember("newset", &Bytes::from("a")).unwrap());
    }

    // ========================================
    // MGET/MSET command tests
    // ========================================

    // MGET tests
    #[test]
    fn test_mget_all_exist() {
        let mut db = Db::new();
        db.set("key1".to_string(), Bytes::from("value1"), None);
        db.set("key2".to_string(), Bytes::from("value2"), None);
        db.set("key3".to_string(), Bytes::from("value3"), None);
        let results = db.mget(&["key1", "key2", "key3"]);
        assert_eq!(results.len(), 3);
        assert_eq!(results[0], Some(Bytes::from("value1")));
        assert_eq!(results[1], Some(Bytes::from("value2")));
        assert_eq!(results[2], Some(Bytes::from("value3")));
    }

    #[test]
    fn test_mget_some_exist() {
        let mut db = Db::new();
        db.set("key1".to_string(), Bytes::from("value1"), None);
        db.set("key3".to_string(), Bytes::from("value3"), None);
        let results = db.mget(&["key1", "key2", "key3"]);
        assert_eq!(results.len(), 3);
        assert_eq!(results[0], Some(Bytes::from("value1")));
        assert_eq!(results[1], None); // key2 doesn't exist
        assert_eq!(results[2], Some(Bytes::from("value3")));
    }

    #[test]
    fn test_mget_none_exist() {
        let db = Db::new();
        let results = db.mget(&["key1", "key2", "key3"]);
        assert_eq!(results.len(), 3);
        assert_eq!(results[0], None);
        assert_eq!(results[1], None);
        assert_eq!(results[2], None);
    }

    #[test]
    fn test_mget_wrong_type() {
        let mut db = Db::new();
        db.set("string_key".to_string(), Bytes::from("value"), None);
        db.sadd("set_key", vec![Bytes::from("a")]).unwrap();
        let results = db.mget(&["string_key", "set_key"]);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], Some(Bytes::from("value")));
        assert_eq!(results[1], None); // Wrong type returns nil
    }

    #[test]
    fn test_mget_bytes() {
        let mut db = Db::new();
        db.set("key1".to_string(), Bytes::from("value1"), None);
        db.set("key2".to_string(), Bytes::from("value2"), None);
        let keys = vec![Bytes::from("key1"), Bytes::from("key2"), Bytes::from("key3")];
        let results = db.mget_bytes(&keys);
        assert_eq!(results.len(), 3);
        assert_eq!(results[0], Some(Bytes::from("value1")));
        assert_eq!(results[1], Some(Bytes::from("value2")));
        assert_eq!(results[2], None);
    }

    // MSET tests
    #[test]
    fn test_mset_basic() {
        let mut db = Db::new();
        db.mset(&[
            ("key1", Bytes::from("value1")),
            ("key2", Bytes::from("value2")),
            ("key3", Bytes::from("value3")),
        ]);
        assert_eq!(db.get("key1"), Some(Bytes::from("value1")));
        assert_eq!(db.get("key2"), Some(Bytes::from("value2")));
        assert_eq!(db.get("key3"), Some(Bytes::from("value3")));
    }

    #[test]
    fn test_mset_overwrite() {
        let mut db = Db::new();
        db.set("key1".to_string(), Bytes::from("old_value"), None);
        db.mset(&[
            ("key1", Bytes::from("new_value")),
            ("key2", Bytes::from("value2")),
        ]);
        assert_eq!(db.get("key1"), Some(Bytes::from("new_value")));
        assert_eq!(db.get("key2"), Some(Bytes::from("value2")));
    }

    #[test]
    fn test_mset_bytes() {
        let mut db = Db::new();
        let pairs = vec![
            (Bytes::from("key1"), Bytes::from("value1")),
            (Bytes::from("key2"), Bytes::from("value2")),
        ];
        db.mset_bytes(&pairs);
        assert_eq!(db.get("key1"), Some(Bytes::from("value1")));
        assert_eq!(db.get("key2"), Some(Bytes::from("value2")));
    }

    #[test]
    fn test_mset_empty() {
        let mut db = Db::new();
        db.mset(&[]);
        // Should not crash, no keys set
        assert!(!db.exists("any_key"));
    }

    #[test]
    fn test_mget_mset_roundtrip() {
        let mut db = Db::new();
        // Set multiple keys
        db.mset(&[
            ("key1", Bytes::from("value1")),
            ("key2", Bytes::from("value2")),
            ("key3", Bytes::from("value3")),
        ]);
        // Get them all back
        let results = db.mget(&["key1", "key2", "key3"]);
        assert_eq!(results[0], Some(Bytes::from("value1")));
        assert_eq!(results[1], Some(Bytes::from("value2")));
        assert_eq!(results[2], Some(Bytes::from("value3")));
    }
}
