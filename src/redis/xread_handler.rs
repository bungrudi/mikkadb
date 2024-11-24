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
                {
                    let redis = self.redis.lock().unwrap();
                    concrete_ids[i] = redis.storage.get_last_stream_id(key)
                        .unwrap_or_else(|| "0-0".to_string());
                }
                
                // If using "0-0", limit to 1 entry
                if concrete_ids[i] == "0-0" {
                    self.request.count = Some(1);
                }
            }
        }

        if let Some(block_ms) = self.request.block {
            // Try immediately first
            let results = self.try_read(&concrete_ids)?;
            if !results.is_empty() {
                return Ok(results);
            }
            
            // block_ms == 0 means block indefinitely
            let block_duration = if block_ms == 0 {
                None // No timeout
            } else {
                Some(Duration::from_millis(block_ms))
            };
            
            let start_time = Instant::now();
            
            // Then wait with longer sleep intervals
            loop {
                // Check if we've exceeded timeout (if any)
                if let Some(timeout) = block_duration {
                    if start_time.elapsed() >= timeout {
                        #[cfg(debug_assertions)]
                        println!("[XReadHandler::run_loop] Block timeout reached after {:?}", start_time.elapsed());
                        return Ok(vec![]);
                    }
                }
                
                // Sleep for a short duration
                let sleep_duration = if let Some(timeout) = block_duration {
                    let remaining = timeout - start_time.elapsed();
                    std::cmp::min(remaining, Duration::from_millis(300))
                } else {
                    Duration::from_millis(300) // For block 0, just use fixed sleep
                };
                thread::sleep(sleep_duration);
                
                let results = self.try_read(&concrete_ids)?;
                if !results.is_empty() {
                    return Ok(results);
                }
            }
        } else {
            // For non-blocking mode, include all streams
            if let Ok(results) = self.try_read(&concrete_ids) {
                Ok(results)
            } else {
                Ok(Vec::new())
            }
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
            println!("\n[XReadHandler::try_read] Processing stream '{}' with ID '{}'", stream_key, ids[i]);

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
                self.request.count,
                self.request.block.is_some(), // Use strict comparison for blocking mode
            );
            
            #[cfg(debug_assertions)]
            println!("  - Found {} entries after {}:{}", entries.len(), ms, seq);
            if !entries.is_empty() {
                #[cfg(debug_assertions)]
                println!("  - Entry IDs: {:?}", entries.iter().map(|e| &e.id).collect::<Vec<_>>());
            }
            results.push((stream_key.clone(), entries));
        }

        drop(redis);

        #[cfg(debug_assertions)]
        println!("\n[XReadHandler::try_read] === Operation summary ===");
        #[cfg(debug_assertions)]
        if results.iter().all(|(_, entries)| entries.is_empty()) {
            println!("No entries found in any stream");
            if self.request.block.is_none() {
                println!("Returning nil (non-blocking mode)");
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
