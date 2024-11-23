use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::thread;

use crate::redis::storage::{Storage, StreamEntry};

pub struct XReadRequest {
    pub keys: Vec<String>,
    pub ids: Vec<String>,
    pub block: Option<u64>,
    pub count: Option<usize>,
}

pub struct XReadHandler {
    storage: Arc<Mutex<Storage>>,
    request: XReadRequest,
    start_time: Instant,
}

impl XReadHandler {
    pub fn new(storage: Arc<Mutex<Storage>>, request: XReadRequest) -> Self {
        Self {
            storage,
            request,
            start_time: Instant::now(),
        }
    }

    fn parse_stream_id(id: &str) -> Result<(u64, u64), String> {
        if id == "$" {
            return Ok((u64::MAX, 0));
        }

        let parts: Vec<&str> = id.split('-').collect();
        match parts.as_slice() {
            [ts, seq] => {
                let timestamp = ts.parse::<u64>()
                    .map_err(|_| "Invalid timestamp in stream ID".to_string())?;
                let sequence = seq.parse::<u64>()
                    .map_err(|_| "Invalid sequence in stream ID".to_string())?;
                Ok((timestamp, sequence))
            }
            [ts] => {
                let timestamp = ts.parse::<u64>()
                    .map_err(|_| "Invalid timestamp in stream ID".to_string())?;
                Ok((timestamp, 0))
            }
            _ => Err("Invalid stream ID format".to_string()),
        }
    }

    fn process_stream(&self, stream_key: &str, id: &str, count: Option<usize>) -> Result<Vec<StreamEntry>, String> {
        let storage = self.storage.lock().unwrap();
        
        let mut effective_id = id.to_string();
        if id == "$" {
            // For $ we need to get the latest ID from the stream
            let entries = storage.xrange(stream_key, "0-0", "+").map_err(|e| e.to_string())?;
            if let Some(last_entry) = entries.last() {
                effective_id = last_entry.id.clone();
            } else {
                effective_id = "0-0".to_string();
            }
        }

        let mut entries = storage.xrange(stream_key, &effective_id, "+")
            .map_err(|e| e.to_string())?;

        // Remove the first entry if it matches our ID (we want entries after this ID)
        if !entries.is_empty() && entries[0].id == effective_id {
            entries.remove(0);
        }

        // Apply COUNT if specified
        if let Some(count) = count {
            entries.truncate(count);
        }

        Ok(entries)
    }

    pub fn run_loop(&mut self) -> Result<Vec<(String, Vec<StreamEntry>)>, String> {
        loop {
            match self.execute()? {
                Some(results) => {
                    // Check if we have enough entries based on COUNT
                    if let Some(count) = self.request.count {
                        let total_entries: usize = results.iter()
                            .map(|(_, entries)| entries.len())
                            .sum();
                        if total_entries >= count {
                            // Trim results to match COUNT exactly
                            let mut trimmed_results = Vec::new();
                            let mut remaining = count;
                            for (key, entries) in results {
                                let take = remaining.min(entries.len());
                                trimmed_results.push((key, entries[..take].to_vec()));
                                remaining = remaining.saturating_sub(take);
                                if remaining == 0 {
                                    break;
                                }
                            }
                            return Ok(trimmed_results);
                        }
                    }
                    return Ok(results);
                }
                None => {
                    if let Some(block) = self.request.block {
                        if self.start_time.elapsed() >= Duration::from_millis(block) {
                            return Ok(Vec::new());
                        }
                        thread::sleep(Duration::from_millis(100));
                        continue;
                    }
                    return Ok(Vec::new());
                }
            }
        }
    }

    fn execute(&self) -> Result<Option<Vec<(String, Vec<StreamEntry>)>>, String> {
        let mut results = Vec::new();
        let mut has_entries = false;

        for (key, id) in self.request.keys.iter().zip(self.request.ids.iter()) {
            let entries = self.process_stream(key, id, self.request.count)?;
            if !entries.is_empty() {
                has_entries = true;
            }
            results.push((key.clone(), entries));
        }

        if has_entries {
            Ok(Some(results))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn create_test_storage() -> Arc<Mutex<Storage>> {
        Arc::new(Mutex::new(Storage::new()))
    }

    #[test]
    fn test_parse_stream_id() {
        assert_eq!(XReadHandler::parse_stream_id("1234-0").unwrap(), (1234, 0));
        assert_eq!(XReadHandler::parse_stream_id("1234-5").unwrap(), (1234, 5));
        assert_eq!(XReadHandler::parse_stream_id("1234").unwrap(), (1234, 0));
        assert_eq!(XReadHandler::parse_stream_id("$").unwrap(), (u64::MAX, 0));
        assert!(XReadHandler::parse_stream_id("invalid").is_err());
    }

    #[test]
    fn test_xread_with_dollar() {
        let storage = create_test_storage();
        
        // Add some test data
        {
            let storage_guard = storage.lock().unwrap();
            let mut fields = HashMap::new();
            fields.insert("temperature".to_string(), "25".to_string());
            storage_guard.xadd("mystream", "1-0", fields.clone()).unwrap();
            
            fields.insert("temperature".to_string(), "26".to_string());
            storage_guard.xadd("mystream", "2-0", fields).unwrap();
        }

        let request = XReadRequest {
            keys: vec!["mystream".to_string()],
            ids: vec!["$".to_string()],
            block: None,
            count: None,
        };

        let handler = XReadHandler::new(storage, request);
        let results = handler.run_loop().unwrap();
        
        assert_eq!(results.len(), 1);
        let (stream_name, entries) = &results[0];
        assert_eq!(stream_name, "mystream");
        assert_eq!(entries.len(), 0); // Should be empty since $ means "new entries only"
    }

    #[test]
    fn test_xread_with_count() {
        let storage = create_test_storage();
        
        // Add test data
        {
            let storage_guard = storage.lock().unwrap();
            let mut fields = HashMap::new();
            fields.insert("temperature".to_string(), "25".to_string());
            storage_guard.xadd("mystream", "1-0", fields.clone()).unwrap();
            
            fields.insert("temperature".to_string(), "26".to_string());
            storage_guard.xadd("mystream", "2-0", fields.clone()).unwrap();
            
            fields.insert("temperature".to_string(), "27".to_string());
            storage_guard.xadd("mystream", "3-0", fields).unwrap();
        }

        let request = XReadRequest {
            keys: vec!["mystream".to_string()],
            ids: vec!["0-0".to_string()],
            block: None,
            count: Some(2),
        };

        let handler = XReadHandler::new(storage, request);
        let results = handler.run_loop().unwrap();
        
        assert_eq!(results.len(), 1);
        let (_, entries) = &results[0];
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].id, "1-0");
        assert_eq!(entries[1].id, "2-0");
    }

    #[test]
    fn test_xread_blocking() {
        let storage = create_test_storage();
        let request = XReadRequest {
            keys: vec!["mystream".to_string()],
            ids: vec!["0-0".to_string()],
            block: Some(100), // 100ms timeout
            count: None,
        };

        let handler = XReadHandler::new(storage.clone(), request);
        let start = Instant::now();
        let results = handler.run_loop().unwrap();
        let elapsed = start.elapsed();

        assert!(elapsed >= Duration::from_millis(100));
        assert_eq!(results.len(), 0); // No data was added, so should be empty
    }
}
