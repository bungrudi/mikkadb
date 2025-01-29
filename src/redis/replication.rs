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
    pub last_sent_offset: u64,
    pub last_acked_offset: Option<u64>,
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
            last_sent_offset: 0,
            last_acked_offset: None,
        };

        let key = format!("{}:{}", host, port);
        #[cfg(debug_assertions)]
        println!("[REPL] Adding new replica: {} (current replication offset: {})", key, self.get_current_offset());
        self.replicas.lock().unwrap().insert(key, replica);
        #[cfg(debug_assertions)]
        println!("[REPL] Total replicas after add: {}", self.replicas.lock().unwrap().len());
    }

    pub fn enqueue_for_replication(&mut self, command: &str) {
        #[cfg(debug_assertions)]
        println!("[REPL] Enqueueing command for replication: {}", command);
        self.command_queue.lock().unwrap().push_back(command.to_string());
    }

    pub fn increment_offset(&mut self) {
        let mut current_offset = self.current_offset.lock().unwrap();
        *current_offset += 1;
        #[cfg(debug_assertions)]
        println!("[REPL] Updated replication offset: {} -> {}", *current_offset - 1, *current_offset);
    }

    pub fn send_pending_commands(&mut self) -> usize {
        let mut queue = self.command_queue.lock().unwrap();
        if !queue.is_empty() {
            #[cfg(debug_assertions)]
            println!("[REPL] Found {} commands in replication queue", queue.len());
            
            // Get all commands first
            let commands: Vec<String> = queue.drain(..).collect();
            let mut sent_count = 0;
            // Send all commands to each replica
            for replica in self.replicas.lock().unwrap().values_mut() {
                for command in &commands {
                    #[cfg(debug_assertions)]
                    println!("[REPL] Sending command to replica: {}", command);
                    
                    match (replica.stream.write_all(command.as_bytes()), replica.stream.flush()) {
                        (Ok(_), Ok(_)) => {
                            // Update replica offset by 1 for each command
                            replica.last_sent_offset += 1;
                            sent_count += 1;
                        }
                        (Err(e), _) | (_, Err(e)) => {
                            #[cfg(debug_assertions)]
                            println!("[REPL] Error sending command to replica: {}", e);
                        }
                    }
                }
            }
            
            sent_count
        } else {
            0
        }
    }

    pub fn send_getack_to_replicas(&self) -> std::io::Result<()> {
        #[cfg(debug_assertions)]
        println!("[REPL] Sending GETACK to replicas");
        
        let mut replicas = self.replicas.lock().unwrap();
        if replicas.is_empty() {
            #[cfg(debug_assertions)]
            println!("[REPL] No replicas to send GETACK to");
            return Ok(());
        }
        
        for replica in replicas.values_mut() {
            if let Some(last_acked_offset) = replica.last_acked_offset {
                #[cfg(debug_assertions)]
                println!("[REPL] Sending GETACK to replica {}:{} (last acked: {})", replica.host, replica.port, last_acked_offset);
            } else {
                #[cfg(debug_assertions)]
                println!("[REPL] Sending GETACK to replica {}:{} (last acked: None)", replica.host, replica.port);
            }
            
            let getack_command = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";
            match (replica.stream.write_all(getack_command.as_bytes()), replica.stream.flush()) {
                (Ok(_), Ok(_)) => {
                    #[cfg(debug_assertions)]
                    println!("[REPL] Successfully sent GETACK to replica {}:{}", replica.host, replica.port);
                },
                (Err(e), _) | (_, Err(e)) => {
                    #[cfg(debug_assertions)]
                    println!("[REPL] Error sending GETACK to replica {}:{}: {}", replica.host, replica.port, e);
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    pub fn update_replica_offset(&mut self, replica_key: &str, offset: u64) {
        if let Some(replica) = self.replicas.lock().unwrap().get_mut(replica_key) {
            // Validate that ACK offset doesn't exceed what we've sent
            if let Some(last_acked_offset) = replica.last_acked_offset {
                if offset > replica.last_sent_offset {
                    #[cfg(debug_assertions)]
                    println!("[REPL] Warning: Replica {} sent invalid ACK offset {} > last_sent_offset {}", 
                        replica_key, offset, replica.last_sent_offset);
                    return;
                }
                #[cfg(debug_assertions)]
                println!("[REPL] Updating replica {} acked offset: {} -> {}", replica_key, last_acked_offset, offset);
            } else {
                #[cfg(debug_assertions)]
                println!("[REPL] Updating replica {} acked offset: None -> Some({})", replica_key, offset);
            }
            
            replica.last_acked_offset = Some(offset);
        }
    }

    pub fn count_up_to_date_replicas_with_offset(&self, offset: u64) -> usize {
        let replicas = self.replicas.lock().unwrap();
        
        #[cfg(debug_assertions)]
        println!("[REPL] Counting up-to-date replicas. Current offset: {}", offset);
        
        // If there are no replicas, return 0 immediately
        if replicas.is_empty() {
            #[cfg(debug_assertions)]
            println!("[REPL] No replicas found");
            return 0;
        }
        
        let mut count = 0;
        for replica in replicas.values() {
            if let Some(last_acked_offset) = replica.last_acked_offset {
                #[cfg(debug_assertions)]
                println!("[REPL] Considering 'replica' {}:{} - acked offset: {} against current: {}", 
                    replica.host, replica.port, last_acked_offset, offset);

                #[cfg(debug_assertions)]
                println!("[REPL] Checking replica {}:{} - acked offset: {} against current: {}", 
                    replica.host, replica.port, last_acked_offset, offset);
                if last_acked_offset >= offset {
                    count += 1;
                    #[cfg(debug_assertions)]
                    println!("[REPL] Replica {}:{} is up to date", replica.host, replica.port);
                } else {
                    #[cfg(debug_assertions)]
                    println!("[REPL] Replica {}:{} is behind (acked offset {} > current {})", 
                        replica.host, replica.port, last_acked_offset, offset);
                }
            } else {
                #[cfg(debug_assertions)]
                println!("[REPL] Replica {}:{} has not acknowledged any offset yet", replica.host, replica.port);
            }
        }
        
        #[cfg(debug_assertions)]
        println!("[REPL] Found {} up-to-date replicas out of {}", count, replicas.len());
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
