use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::thread;

use crate::redis::Redis;
use crate::redis::storage::{StreamEntry, Storage};

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

        // Convert $ to appropriate ID
        let mut concrete_ids = self.request.ids.clone();
        for (i, (key, id)) in self.request.keys.iter().zip(&self.request.ids).enumerate() {
            if id == "$" {
                // For $ ID, use the last ID in the stream or "0-0" if stream is empty
                let redis = self.redis.lock().unwrap();
                concrete_ids[i] = redis.storage.get_last_stream_id(key)
                    .unwrap_or_else(|| "0-0".to_string());
                #[cfg(debug_assertions)]
                println!("[XReadHandler::run_loop] key={}, id={}, concrete_id={}", key, id, concrete_ids[i]);
            }
        }

        if let Some(block_ms) = self.request.block {
            let start_time = Instant::now();
            
            // block_ms == 0 means block indefinitely
            let block_duration = if block_ms == 0 {
                None // No timeout
            } else {
                Some(Duration::from_millis(block_ms))
            };
            
            // Keep checking until timeout
            loop {
                // Check for timeout first
                if let Some(timeout) = block_duration {
                    let elapsed = start_time.elapsed();
                    if elapsed >= timeout {
                        #[cfg(debug_assertions)]
                        println!("[XReadHandler::run_loop] Block timeout reached after {:?}", elapsed);
                        return Ok(vec![]); // Return empty vec ONLY on timeout
                    }
                }

                // Try reading data
                let results = self.try_read(&concrete_ids)?;
                if !results.is_empty() {
                    return Ok(results); // Return non-empty results immediately
                }

                // Sleep for a short duration to avoid busy waiting
                thread::sleep(Duration::from_millis(10));
            }
        } else {
            // For non-blocking mode, just try once
            let results = self.try_read(&concrete_ids)?;
            #[cfg(debug_assertions)]
            println!("[XReadHandler::run_loop] Non-blocking mode results: {:?}", results);
            Ok(results)
        }
    }

    fn try_read(&self, ids: &[String]) -> Result<Vec<(String, Vec<StreamEntry>)>, String> {
        #[cfg(debug_assertions)]
        println!("\n[XReadHandler::try_read] === Starting read operation ===");
        #[cfg(debug_assertions)]
        println!("Request parameters:");
        #[cfg(debug_assertions)]
        println!("  - Keys: {:?}", self.request.keys);
        #[cfg(debug_assertions)]
        println!("  - IDs: {:?}", ids);
        #[cfg(debug_assertions)]
        println!("  - Block: {:?} ({})", 
            self.request.block,
            if self.request.block.is_some() { "BLOCKING" } else { "NON-BLOCKING" }
        );
        #[cfg(debug_assertions)]
        println!("  - Count: {:?}", self.request.count);

        let mut results = Vec::new();
        let redis = self.redis.lock().unwrap();

        for (i, stream_key) in self.request.keys.iter().enumerate() {
            #[cfg(debug_assertions)]
            println!("[XReadHandler::try_read] Processing stream '{}' with ID '{}'", stream_key, ids[i]);

            let (ms, seq) = match Storage::parse_stream_id(&ids[i]) {
                Ok((ms, seq)) => {
                    #[cfg(debug_assertions)]
                    println!("  - Parsed ID {}:{}", ms, seq);
                    (ms, seq)
                },
                Err(e) => {
                    #[cfg(debug_assertions)]
                    println!("  - Failed to parse ID: {}", e);
                    return Err(e);
                }
            };

            // Get entries that arrived after the specified ID
            let entries = redis.storage.get_stream_entries(
                stream_key,
                ms,
                seq,
                self.request.count
            );
            
            #[cfg(debug_assertions)]
            println!("  - Found {} entries after {}:{}", entries.len(), ms, seq);

            // Only include streams that have new entries
            if !entries.is_empty() {
                results.push((stream_key.clone(), entries));
            }
        }

        drop(redis);

        #[cfg(debug_assertions)]
        println!("\n[XReadHandler::try_read] === Operation summary ===");
        #[cfg(debug_assertions)]
        if results.is_empty() {
            println!("No entries found in any stream");
            if self.request.block.is_none() {
                println!("Returning empty result (non-blocking mode)");
            } else {
                println!("Will continue waiting (blocking mode)");
            }
        } else {
            println!("Found entries in {} streams:", results.len());
            for (stream, entries) in &results {
                println!("  - {}: {} entries", stream, entries.len());
            }
        }

        Ok(results)
    }
}
