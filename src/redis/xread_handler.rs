use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::thread;

use crate::redis::Redis;
use crate::redis::storage::StreamEntry;

pub struct XReadRequest {
    pub keys: Vec<String>,
    pub ids: Vec<String>,
    pub block: Option<u64>,
    pub count: Option<usize>,
}

pub struct XReadHandler {
    redis: Arc<Mutex<Redis>>,
    request: XReadRequest,
}

impl XReadHandler {
    pub fn new(redis: Arc<Mutex<Redis>>, request: XReadRequest) -> Self {
        XReadHandler {
            redis,
            request,
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

    fn process_stream(&self, stream_key: &str, id: &str, count: Option<usize>) -> Result<Vec<StreamEntry>, String> {
        let (ms, seq) = Self::parse_stream_id(id)?;
        
        let redis = self.redis.lock().unwrap();
        let entries = redis.storage.get_stream_entries(stream_key, ms, seq, count);
        
        Ok(entries)
    }

    pub fn run_loop(&mut self) -> Result<Vec<(String, Vec<StreamEntry>)>, String> {
        let start_time = Instant::now();
        
        loop {
            if let Some(results) = self.execute()? {
                return Ok(results);
            }

            if let Some(block_ms) = self.request.block {
                let elapsed = start_time.elapsed();
                if elapsed >= Duration::from_millis(block_ms) {
                    return Ok(vec![]);
                }
                thread::sleep(Duration::from_millis(10));
            } else {
                return Ok(vec![]);
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
