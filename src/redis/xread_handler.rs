use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
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

        if let Some(block_ms) = self.request.block {
            let start_time = Instant::now();
            let block_duration = Duration::from_millis(block_ms);
            
            // Try immediately first
            let results = self.try_read()?;
            if !results.is_empty() {
                return Ok(results);
            }
            
            // Then wait with longer sleep intervals
            while start_time.elapsed() < block_duration {
                let remaining = block_duration - start_time.elapsed();
                let sleep_duration = std::cmp::min(remaining, Duration::from_millis(50));
                thread::sleep(sleep_duration);
                
                let results = self.try_read()?;
                if !results.is_empty() {
                    return Ok(results);
                }
            }
            
            #[cfg(debug_assertions)]
            println!("[XReadHandler::run_loop] Block timeout reached after {:?}", start_time.elapsed());
            return Ok(vec![]);
        } else {
            #[cfg(debug_assertions)]
            println!("[XReadHandler::run_loop] Non-blocking mode, calling try_read once");
            self.try_read()
        }
    }

    fn try_read(&self) -> Result<Vec<(String, Vec<StreamEntry>)>, String> {
        #[cfg(debug_assertions)]
        println!("\n[XReadHandler::try_read] === Starting read operation ===");
        #[cfg(debug_assertions)]
        println!("Request parameters:");
        #[cfg(debug_assertions)]
        println!("  - Keys: {:?}", self.request.keys);
        #[cfg(debug_assertions)]
        println!("  - IDs: {:?}", self.request.ids);
        #[cfg(debug_assertions)]
        println!("  - Block: {:?} ({})", 
            self.request.block,
            if self.request.block.is_some() { "BLOCKING" } else { "NON-BLOCKING" }
        );
        #[cfg(debug_assertions)]
        println!("  - Count: {:?}", self.request.count);

        let mut results = Vec::new();
        let redis = self.redis.lock().unwrap();

        for (stream_key, stream_id) in self.request.keys.iter().zip(self.request.ids.iter()) {
            #[cfg(debug_assertions)]
            println!("\n[XReadHandler::try_read] Processing stream '{}' with ID '{}'", stream_key, stream_id);

            if stream_id == "$" {
                #[cfg(debug_assertions)]
                println!("[XReadHandler::try_read] Special $ ID handling:");

                if self.request.block.is_none() {
                    #[cfg(debug_assertions)]
                    println!("  - Non-blocking mode with $ ID");
                    #[cfg(debug_assertions)]
                    println!("  - Skipping stream (Redis returns nil for $ in non-blocking mode)");
                    continue;
                }

                #[cfg(debug_assertions)]
                println!("  - Blocking mode with $ ID");
                
                let last_id = redis.storage.get_last_stream_id(stream_key);
                #[cfg(debug_assertions)]
                println!("  - Last stream ID: {:?}", last_id);

                let (ms, seq) = if let Some(last_id) = last_id {
                    let (last_ms, last_seq) = Storage::parse_stream_id(&last_id)?;
                    #[cfg(debug_assertions)]
                    println!("  - Using last ID as starting point: {}:{}", last_ms, last_seq);
                    (last_ms, last_seq)
                } else {
                    #[cfg(debug_assertions)]
                    println!("  - No entries in stream, using 0:0 as starting point");
                    (0, 0)
                };

                // Get entries strictly after our last ID
                let entries = redis.storage.get_stream_entries(stream_key, ms, seq, self.request.count);
                
                #[cfg(debug_assertions)]
                println!("  - Found {} entries after {}:{}", entries.len(), ms, seq);
                if !entries.is_empty() {
                    #[cfg(debug_assertions)]
                    println!("  - Entry IDs: {:?}", entries.iter().map(|e| &e.id).collect::<Vec<_>>());
                }

                if !entries.is_empty() {
                    results.push((stream_key.clone(), entries));
                }
            } else {
                #[cfg(debug_assertions)]
                println!("[XReadHandler::try_read] Regular ID handling:");
                
                let (ms, seq) = match Storage::parse_stream_id(stream_id) {
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

                let entries = redis.storage.get_stream_entries(stream_key, ms, seq, self.request.count);
                
                #[cfg(debug_assertions)]
                println!("  - Found {} entries after {}:{}", entries.len(), ms, seq);
                if !entries.is_empty() {
                    #[cfg(debug_assertions)]
                    println!("  - Entry IDs: {:?}", entries.iter().map(|e| &e.id).collect::<Vec<_>>());
                }

                if !entries.is_empty() {
                    results.push((stream_key.clone(), entries));
                }
            }
        }

        #[cfg(debug_assertions)]
        println!("\n[XReadHandler::try_read] === Operation summary ===");
        #[cfg(debug_assertions)]
        if results.is_empty() {
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
