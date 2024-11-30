use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use crate::redis::replica::Replica;
use std::io::{Read, Write, Result};
use std::thread;
use std::time::Duration;
use std::sync::Arc;
use std::net::SocketAddr;

pub trait TcpStreamTrait: Read + Write + Send + 'static {
    fn peer_addr(&self) -> Result<SocketAddr>;
    fn try_clone(&self) -> Result<Box<dyn TcpStreamTrait>>;
}

impl TcpStreamTrait for std::net::TcpStream {
    fn peer_addr(&self) -> Result<SocketAddr> {
        self.peer_addr()
    }

    fn try_clone(&self) -> Result<Box<dyn TcpStreamTrait>> {
        Ok(Box::new(self.try_clone()?))
    }
}

pub struct ReplicationManager {
    pub replicas: Mutex<HashMap<String, Replica>>,
    pub replica_queue: Mutex<VecDeque<String>>,
    replication_offset: AtomicU64, // bytes sent to replica (as master)
}

impl ReplicationManager {
    pub fn new() -> Self {
        ReplicationManager {
            replicas: Mutex::new(HashMap::new()),
            replica_queue: Mutex::new(VecDeque::new()),
            replication_offset: AtomicU64::new(0),
        }
    }

    pub fn enqueue_for_replication(&self, command: &str) {
        #[cfg(debug_assertions)]
        println!("[REPL] Enqueueing command for replication: {}", command);
        
        self.replica_queue.lock().unwrap().push_back(command.to_string());
        let new_offset = self.replication_offset.fetch_add(command.len() as u64, Ordering::SeqCst);
        
        #[cfg(debug_assertions)]
        println!("[REPL] Updated replication offset: {} -> {}", new_offset, new_offset + command.len() as u64);
    }

    pub fn update_replica_offset(&self, replica_key: &str, offset: u64) {
        if let Some(replica) = self.replicas.lock().unwrap().get_mut(replica_key) {
            #[cfg(debug_assertions)]
            println!("[REPL] Updating replica {} offset: {} -> {}", replica_key, replica.offset.load(Ordering::SeqCst), offset);
            replica.offset.store(offset, Ordering::SeqCst);
        } else {
            #[cfg(debug_assertions)]
            println!("[REPL] Error: Replica not found while updating offset: {}", replica_key);
        }
    }

    pub fn get_replication_offset(&self) -> u64 {
        self.replication_offset.load(Ordering::SeqCst)
    }

    pub fn count_up_to_date_replicas(&self) -> usize {
        let current_offset = self.replication_offset.load(Ordering::SeqCst);
        let count = self.replicas
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, replica)| {
                let replica_offset = replica.offset.load(Ordering::SeqCst);
                #[cfg(debug_assertions)]
                println!("[REPL] Checking replica {} offset: {} against current: {}", 
                    key, replica_offset, current_offset);
                replica_offset >= current_offset
            })
            .count();
        
        #[cfg(debug_assertions)]
        println!("[REPL] Found {} up-to-date replicas", count);
        
        count
    }

    pub fn add_replica(&mut self, host: String, port: String, stream: Box<dyn TcpStreamTrait>) {
        let replica_key = format!("{}:{}", host, port);
        let replica = Replica::new(host, port, stream);
        #[cfg(debug_assertions)]
        println!("[REPL] Adding new replica: {} (current replication offset: {})", replica_key, self.get_replication_offset());
        #[cfg(debug_assertions)]
        println!("[REPL] Total replicas after add: {}", self.replicas.lock().unwrap().len() + 1);
        self.replicas.lock().unwrap().insert(replica_key, replica);
    }

    pub fn send_pending_commands(&self) -> bool {
        let mut sent_commands = false;
        let mut replica_queue = self.replica_queue.lock().unwrap();
        
        if !replica_queue.is_empty() {
            #[cfg(debug_assertions)]
            println!("[REPL] Found {} commands in replication queue", replica_queue.len());
            
            while let Some(replica_command) = replica_queue.pop_front() {
                let replicas = self.replicas.lock().unwrap();
                #[cfg(debug_assertions)]
                println!("[REPL] Sending command to {} replicas: {}", replicas.len(), replica_command);
                
                for (_key, replica) in &*replicas {
                    let mut stream = replica.stream.lock().unwrap();
                    if let Err(e) = stream.write(replica_command.as_bytes()) {
                        #[cfg(debug_assertions)]
                        eprintln!("[REPL] Error writing to replica {}: {}", _key, e);
                    }
                }
                sent_commands = true;
            }
        }
        sent_commands
    }

    pub fn send_getack_to_replicas(&self) -> std::io::Result<()> {
        #[cfg(debug_assertions)]
        println!("[REPL] Sending GETACK to replicas");
        
        let replicas = self.replicas.lock().unwrap();
        for (_key, replica) in &*replicas {
            let mut stream = replica.stream.lock().unwrap();
            stream.write_all(b"*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n")?;
        }
        Ok(())
    }

    pub fn get_replicas(&self) -> std::sync::MutexGuard<HashMap<String, Replica>> {
        self.replicas.lock().unwrap()
    }

    pub fn start_replication_sync(redis: Arc<Mutex<crate::redis::Redis>>) {
        thread::spawn(move || {
            let mut last_getack = std::time::Instant::now();
            loop {
                {
                    let redis = redis.lock().unwrap();
                    redis.replication.send_pending_commands();
                    
                    // Send GETACK every 10 seconds
                    if last_getack.elapsed() >= Duration::from_secs(10) {
                        #[cfg(debug_assertions)]
                        println!("[REPL] Sending periodic GETACK");
                        
                        if let Err(e) = redis.replication.send_getack_to_replicas() {
                            #[cfg(debug_assertions)]
                            println!("[REPL] Error sending periodic GETACK: {}", e);
                        }
                        last_getack = std::time::Instant::now();
                    }
                }
                thread::sleep(Duration::from_millis(10));
            }
        });
    }
}
