use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::io::{Read, Write, Result};
use std::thread;
use std::time::Duration;
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

pub struct Replica {
    pub host: String,
    pub port: String,
    pub stream: Box<dyn TcpStreamTrait>,
    pub offset: u64,
}

pub struct ReplicationManager {
    replicas: Arc<Mutex<HashMap<String, Replica>>>,
    command_queue: Arc<Mutex<VecDeque<String>>>,
    current_offset: Arc<Mutex<u64>>,
}

impl ReplicationManager {
    pub fn new() -> Self {
        ReplicationManager {
            replicas: Arc::new(Mutex::new(HashMap::new())),
            command_queue: Arc::new(Mutex::new(VecDeque::new())),
            current_offset: Arc::new(Mutex::new(0)),
        }
    }

    pub fn add_replica(&mut self, host: String, port: String, stream: Box<dyn TcpStreamTrait>) {
        let replica = Replica {
            host: host.clone(),
            port: port.clone(),
            stream,
            offset: 0,
        };

        let key = format!("{}:{}", host, port);
        #[cfg(debug_assertions)]
        println!("[REPL] Adding new replica: {} (current replication offset: {})", key, self.get_current_offset());
        self.replicas.lock().unwrap().insert(key, replica);
        #[cfg(debug_assertions)]
        println!("[REPL] Total replicas after add: {}", self.replicas.lock().unwrap().len());
    }

    pub fn enqueue_for_replication(&mut self, command: &str) {
        let mut current_offset = self.current_offset.lock().unwrap();
        *current_offset += command.len() as u64;
        #[cfg(debug_assertions)]
        println!("[REPL] Updated replication offset: {} -> {}", *current_offset - command.len() as u64, *current_offset);

        #[cfg(debug_assertions)]
        println!("[REPL] Enqueueing command for replication: {}", command);
        self.command_queue.lock().unwrap().push_back(command.to_string());
    }

    pub fn send_pending_commands(&mut self) -> usize {
        let mut queue = self.command_queue.lock().unwrap();
        if !queue.is_empty() {
            #[cfg(debug_assertions)]
            println!("[REPL] Found {} commands in replication queue", queue.len());
            while let Some(command) = queue.pop_front() {
                #[cfg(debug_assertions)]
                println!("[REPL] Sending command to {} replicas: {}", self.replicas.lock().unwrap().len(), command);
                for replica in self.replicas.lock().unwrap().values_mut() {
                    let _ = replica.stream.write_all(command.as_bytes());
                    let _ = replica.stream.flush();
                }
            }
        }
        queue.len()
    }

    pub fn send_getack_to_replicas(&self) -> std::io::Result<()> {
        #[cfg(debug_assertions)]
        println!("[REPL] Sending GETACK to replicas");
        for replica in self.replicas.lock().unwrap().values_mut() {
            let getack_command = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";
            replica.stream.write_all(getack_command.as_bytes())?;
            replica.stream.flush()?;
        }
        Ok(())
    }

    pub fn update_replica_offset(&mut self, replica_key: &str, offset: u64) {
        if let Some(replica) = self.replicas.lock().unwrap().get_mut(replica_key) {
            #[cfg(debug_assertions)]
            println!("[REPL] Updating replica {} offset: {} -> {}", replica_key, replica.offset, offset);
            replica.offset = offset;
        }
    }

    pub fn count_up_to_date_replicas(&self) -> usize {
        let current_offset = *self.current_offset.lock().unwrap();
        let mut count = 0;
        for replica in self.replicas.lock().unwrap().values() {
            #[cfg(debug_assertions)]
            println!("[REPL] Checking replica {}:{} offset: {} against current: {}", 
                replica.host, replica.port, replica.offset, current_offset);
            if replica.offset >= current_offset {
                count += 1;
            }
        }
        #[cfg(debug_assertions)]
        println!("[REPL] Found {} up-to-date replicas", count);
        count
    }

    pub fn get_current_offset(&self) -> u64 {
        *self.current_offset.lock().unwrap()
    }

    // #[allow(dead_code)]
    // pub fn get_replicas(&self) -> std::sync::MutexGuard<HashMap<String, Replica>> {
    //     self.replicas.lock().unwrap()
    // }

    pub fn start_replication_sync(redis: Arc<Mutex<crate::redis::Redis>>) {
        thread::spawn(move || {
            let mut last_getack = std::time::Instant::now();
            loop {
                {
                    let mut redis = redis.lock().unwrap();
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
