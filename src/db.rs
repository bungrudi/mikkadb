use std::collections::HashMap;
use bytes::Bytes;
use std::time::{Instant, Duration};

#[derive(Clone, Debug, PartialEq)]
pub struct StreamEntry {
    pub id: (u64, u64), // (milliseconds, sequence)
    pub fields: Vec<(String, String)>,
}

#[derive(Clone)]
enum DataType {
    String(Bytes, Option<Instant>),
    Stream(Vec<StreamEntry>),
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

    pub fn set(&mut self, key: String, value: Bytes, px: Option<u64>) {
        let expiry = px.map(|ms| Instant::now() + Duration::from_millis(ms));
        self.data.insert(key, DataType::String(value, expiry));
    }

    pub fn get(&mut self, key: &str) -> Option<Bytes> {
        if let Some(data_type) = self.data.get(key) {
            match data_type {
                DataType::String(value, expiry) => {
                    if let Some(expiry_time) = expiry {
                        if Instant::now() > *expiry_time {
                            self.data.remove(key);
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
        if let Some(DataType::Stream(entries)) = self.data.get(key) {
            entries.last().map(|e| e.id)
        } else {
            None
        }
    }
    
    pub fn read_stream(&self, key: &str, start_id: (u64, u64)) -> Option<Vec<StreamEntry>> {
        if let Some(DataType::Stream(entries)) = self.data.get(key) {
            let result: Vec<StreamEntry> = entries.iter()
                .filter(|e| e.id.0 > start_id.0 || (e.id.0 == start_id.0 && e.id.1 > start_id.1))
                .cloned()
                .collect();
            
            if result.is_empty() {
                None
            } else {
                Some(result)
            }
        } else {
            None
        }
    }

    pub fn range_stream(&self, key: &str, start: (u64, u64), end: (u64, u64)) -> Option<Vec<StreamEntry>> {
        if let Some(DataType::Stream(entries)) = self.data.get(key) {
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
    }
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
