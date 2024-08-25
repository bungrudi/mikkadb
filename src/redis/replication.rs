use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use crate::redis::replica::Replica;
use std::net::TcpStream;

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
        // self.set_enqueue_getack(true);
    }

    pub fn update_replica_offset(&self, replica_key: &str, offset: u64) {
        if let Some(replica) = self.replicas.lock().unwrap().get_mut(replica_key) {
            println!("updateing offset for replica: {} offset: {}", replica_key, offset);
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

    pub fn add_replica(&mut self, host: String, port: String, stream: TcpStream) {
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
}