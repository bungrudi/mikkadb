use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::thread;

use crate::redis::Redis;
use crate::redis::storage::{StreamEntry, ValueWrapper};

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
        let redis = self.redis.lock().unwrap();
        let storage = &redis.storage;

        if id == "$" {
            // For $ ID, we need to get the last entry's ID
            let (ms, seq) = match storage.get_last_stream_id(stream_key) {
                Some(last_id) => Self::parse_stream_id(&last_id)?,
                None => (0, 0),
            };
            Ok(storage.get_stream_entries(stream_key, ms, seq, count))
        } else {
            let (ms, seq) = Self::parse_stream_id(id)?;
            Ok(storage.get_stream_entries(stream_key, ms, seq, count))
        }
    }

    pub fn run_loop(&mut self) -> Result<Vec<(String, Vec<StreamEntry>)>, String> {
        let start_time = Instant::now();
        loop {
            let mut result = Vec::new();
            let mut has_data = false;

            // Process each stream
            for (key, id) in self.request.keys.iter().zip(self.request.ids.iter()) {
                // Always include the stream in result, even if empty
                match self.process_stream(key, id, self.request.count) {
                    Ok(entries) => {
                        has_data = has_data || !entries.is_empty();
                        result.push((key.clone(), entries));
                    }
                    Err(_) => {
                        result.push((key.clone(), vec![]));
                    }
                }
            }

            // For non-blocking operations, return the result immediately
            if self.request.block.is_none() {
                println!("Non-blocking XREAD returning result with {} streams", result.len());
                return Ok(result);
            }

            // For blocking operations, only return if we have data
            if has_data {
                println!("Blocking XREAD found data, returning result with {} streams", result.len());
                return Ok(result);
            }

            // Check if we've exceeded the blocking timeout
            if let Some(block) = self.request.block {
                let elapsed = start_time.elapsed();
                if elapsed >= Duration::from_millis(block) {
                    println!("Blocking XREAD timed out after {}ms", block);
                    return Ok(vec![]); // Return empty array on timeout
                }
            }

            // Sleep briefly before next iteration
            thread::sleep(Duration::from_millis(10));
        }
    }
}
