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
}

#[derive(Clone)]
pub struct Db {
    data: HashMap<String, DataType>,
}

impl Db {
    pub fn new() -> Self {
        Db {
            data: HashMap::new(),
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
                }
            }
            None => "none".to_string(),
        }
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
}
