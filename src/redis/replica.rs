use std::net::TcpStream;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::sync::Mutex;

pub struct Replica {
    #[allow(dead_code)]
    pub(crate) host: String,
    #[allow(dead_code)]
    pub(crate) port: String,
    pub(crate) stream: Arc<Mutex<TcpStream>>,
    pub(crate) offset: AtomicU64,   
}

impl Replica {
    pub fn new(host: String, port: String, stream: TcpStream) -> Self {
        Self {
            host,
            port,
            stream: Arc::new(Mutex::new(stream)),
            offset: AtomicU64::new(0),
        }
    }
}