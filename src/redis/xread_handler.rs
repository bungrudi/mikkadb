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

    pub fn run_loop(&mut self) -> Result<Vec<(String, Vec<StreamEntry>)>, String> {
        #[cfg(debug_assertions)]
        println!("\n[XReadHandler::run_loop] Starting with block={:?}, count={:?}", self.request.block, self.request.count);
        
        if let Some(block_ms) = self.request.block {
            let start = Instant::now();
            let mut last_try = self.try_read()?;
            #[cfg(debug_assertions)]
            println!("[XReadHandler::run_loop] Initial try_read result: {:?} entries", last_try.len());

            while start.elapsed() < Duration::from_millis(block_ms as u64) {
                if !last_try.is_empty() {
                    #[cfg(debug_assertions)]
                    println!("[XReadHandler::run_loop] Found entries, returning");
                    return Ok(last_try);
                }
                thread::sleep(Duration::from_millis(10));
                last_try = self.try_read()?;
                #[cfg(debug_assertions)]
                println!("[XReadHandler::run_loop] Retried try_read result: {:?} entries", last_try.len());
            }
            
            #[cfg(debug_assertions)]
            println!("[XReadHandler::run_loop] Timeout reached, returning empty array");
            Ok(vec![])
        } else {
            #[cfg(debug_assertions)]
            println!("[XReadHandler::run_loop] Non-blocking mode, calling try_read once");
            self.try_read()
        }
    }

    fn try_read(&self) -> Result<Vec<(String, Vec<StreamEntry>)>, String> {
        #[cfg(debug_assertions)]
        println!("\n[XReadHandler::try_read] Starting read for keys={:?}", self.request.keys);
        let redis = self.redis.lock().map_err(|e| e.to_string())?;
        let mut results = Vec::new();

        for (stream_idx, key) in self.request.keys.iter().enumerate() {
            let id = &self.request.ids[stream_idx];
            #[cfg(debug_assertions)]
            println!("[XReadHandler::try_read] Processing stream '{}' with ID '{}'", key, id);
            
            // Handle $ ID - only return entries after current last ID
            let entries = if id == "$" {
                #[cfg(debug_assertions)]
                println!("[XReadHandler::try_read] Handling $ ID for stream '{}'", key);
                // Get all entries after the current last ID
                let last_id = redis.storage.get_last_stream_id(key).unwrap_or_else(|| "0-0".to_string());
                #[cfg(debug_assertions)]
                println!("[XReadHandler::try_read] Last ID for stream '{}' is '{}'", key, last_id);
                let (ms, seq) = match last_id.split_once('-') {
                    Some((ms_str, seq_str)) => {
                        let ms = ms_str.parse::<u64>().map_err(|_| "Invalid stream ID format")?;
                        let seq = seq_str.parse::<u64>().map_err(|_| "Invalid stream ID format")?;
                        (ms, seq)
                    }
                    None => (0, 0),
                };
                redis.storage.get_stream_entries(key, ms, seq, self.request.count)
            } else {
                #[cfg(debug_assertions)]
                println!("[XReadHandler::try_read] Handling specific ID '{}' for stream '{}'", id, key);
                // Parse the ID
                let (ms, seq) = match id.split_once('-') {
                    Some((ms_str, seq_str)) => {
                        let ms = ms_str.parse::<u64>().map_err(|_| "Invalid stream ID format")?;
                        let seq = seq_str.parse::<u64>().map_err(|_| "Invalid stream ID format")?;
                        (ms, seq)
                    }
                    None => {
                        let ms = id.parse::<u64>().map_err(|_| "Invalid stream ID format")?;
                        (ms, 0)
                    }
                };

                redis.storage.get_stream_entries(key, ms, seq, self.request.count)
            };

            #[cfg(debug_assertions)]
            println!("[XReadHandler::try_read] Got {} entries for stream '{}'", entries.len(), key);
            if !entries.is_empty() {
                #[cfg(debug_assertions)]
                println!("[XReadHandler::try_read] Adding stream '{}' to results", key);
                results.push((key.clone(), entries));
            }
        }

        #[cfg(debug_assertions)]
        println!("[XReadHandler::try_read] Returning {} streams with entries", results.len());
        Ok(results)
    }
}
