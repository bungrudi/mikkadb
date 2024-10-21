use std::sync::{Arc, Mutex};
use std::sync::atomic::AtomicU64;
use crate::redis::replication::TcpStreamTrait;

pub struct Replica {
    pub host: String,
    pub port: String,
    pub stream: Arc<Mutex<Box<dyn TcpStreamTrait>>>,
    pub offset: AtomicU64,
}

impl Replica {
    pub fn new(host: String, port: String, stream: Box<dyn TcpStreamTrait>) -> Self {
        Replica {
            host,
            port,
            stream: Arc::new(Mutex::new(stream)),
            offset: AtomicU64::new(0),
        }
    }
}
