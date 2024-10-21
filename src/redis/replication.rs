use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
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
    enqueue_getack: AtomicBool,
}

impl ReplicationManager {
    pub fn new() -> Self {
        ReplicationManager {
            replicas: Mutex::new(HashMap::new()),
            replica_queue: Mutex::new(VecDeque::new()),
            replication_offset: AtomicU64::new(0),
            enqueue_getack: AtomicBool::new(false),
        }
    }

    pub fn enqueue_for_replication(&self, command: &str) {
        self.replica_queue.lock().unwrap().push_back(command.to_string());
        self.replication_offset.fetch_add(command.len() as u64, Ordering::SeqCst);
    }

    pub fn update_replica_offset(&self, replica_key: &str, offset: u64) {
        if let Some(replica) = self.replicas.lock().unwrap().get_mut(replica_key) {
            println!("updating offset for replica: {} offset: {}", replica_key, offset);
            replica.offset.store(offset, Ordering::SeqCst);
        } else {
            println!("Replica not found: {}", replica_key);
        }
    }

    pub fn get_replication_offset(&self) -> u64 {
        self.replication_offset.load(Ordering::SeqCst)
    }

    pub fn count_up_to_date_replicas(&self) -> usize {
        let current_offset = self.replication_offset.load(Ordering::SeqCst);
        self.replicas
            .lock()
            .unwrap()
            .values()
            .filter(|replica| replica.offset.load(Ordering::SeqCst) >= current_offset)
            .count()
    }

    pub fn add_replica(&mut self, host: String, port: String, stream: Box<dyn TcpStreamTrait>) {
        let replica_key = format!("{}:{}", host, port);
        let replica = Replica::new(host, port, stream);
        println!("adding replica: {}", replica_key);
        self.replicas.lock().unwrap().insert(replica_key, replica);
    }

    pub fn should_send_getack(&self) -> bool {
        self.enqueue_getack.load(Ordering::SeqCst)
    }

    pub fn set_enqueue_getack(&self, value: bool) {
        self.enqueue_getack.store(value, Ordering::SeqCst)
    }

    pub fn get_replicas(&self) -> std::sync::MutexGuard<HashMap<String, Replica>> {
        self.replicas.lock().unwrap()
    }

    pub fn start_replication_sync(redis: Arc<Mutex<crate::redis::Redis>>) {
        thread::spawn(move || {
            loop {
                {
                    let redis = redis.lock().unwrap();
                    {
                        let mut replica_queue = redis.replication.replica_queue.lock().unwrap();
                        while let Some(replica_command) = replica_queue.pop_front() {
                            let replicas = redis.replication.replicas.lock().unwrap();
                            println!("replicas size: {}", replicas.values().len());
                            for (key, replica) in &*replicas {
                                println!("sending replica command: {} \r\n to {} {}:{}", replica_command, key, &replica.host, &replica.port);
                                let mut stream = replica.stream.lock().unwrap();
                                let result = stream.write(replica_command.as_bytes());
                                if result.is_err() {
                                    eprintln!("error writing to replica: {}", result.err().unwrap());
                                }
                            }
                        }
                    }
                    // send "REPLCONF GETACK *" after each propagation
                    if redis.replication.should_send_getack() {
                        println!("sending GETACK to replicas");
                        for replica in redis.replication.get_replicas().values() {
                            let mut stream = replica.stream.lock().unwrap();
                            let result = stream.write(b"*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n");
                            if result.is_err() {
                                eprintln!("error writing to replica: {}", result.err().unwrap());
                            }
                        }
                        redis.replication.set_enqueue_getack(false);
                    }
                } // expecting the lock to be dropped here
                thread::sleep(Duration::from_millis(10));
            }
        });
    }
}
