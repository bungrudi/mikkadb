use std::io::{Read, Write, Result};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use redis_starter_rust::client_handler::ClientHandler;

#[derive(Clone)]
pub struct MockTcpStream {
    // What client writes, server reads
    pub read_data: Arc<Mutex<Vec<u8>>>,
    // What server writes, client reads
    pub write_data: Arc<Mutex<Vec<u8>>>,
    pub is_server: bool,
    pub shutdown: Arc<Mutex<bool>>,
}

impl MockTcpStream {
    pub fn new() -> Self {
        let (client, _) = Self::new_pair();
        client
    }

    pub fn new_pair() -> (Self, Self) {
        let read_data = Arc::new(Mutex::new(Vec::new()));
        let write_data = Arc::new(Mutex::new(Vec::new()));
        let shutdown = Arc::new(Mutex::new(false));

        let client = MockTcpStream {
            read_data: write_data.clone(),
            write_data: read_data.clone(),
            is_server: false,
            shutdown: shutdown.clone(),
        };

        let server = MockTcpStream {
            read_data: read_data,
            write_data: write_data,
            is_server: true,
            shutdown,
        };

        (client, server)
    }

    pub fn wait_for_write(&self, pattern: &str, timeout_ms: u64) -> bool {
        self.wait_for_pattern(pattern, timeout_ms)
    }

    pub fn wait_for_pattern(&self, pattern: &str, timeout_ms: u64) -> bool {
        let start_time = Instant::now();
        let pattern_bytes = pattern.as_bytes();
        
        loop {
            if start_time.elapsed() > Duration::from_millis(timeout_ms) {
                #[cfg(debug_assertions)]
                println!("[MockTcpStream::wait_for_pattern] Timeout after {}ms", timeout_ms);
                return false;
            }

            if *self.shutdown.lock().unwrap() {
                #[cfg(debug_assertions)]
                println!("[MockTcpStream::wait_for_pattern] Stream is shutdown");
                return false;
            }

            let data = self.read_data.lock().unwrap();
            if data.windows(pattern_bytes.len()).any(|window| window == pattern_bytes) {
                #[cfg(debug_assertions)]
                println!("[MockTcpStream::wait_for_pattern] Pattern found");
                return true;
            }
            
            #[cfg(debug_assertions)]
            // print current data and sleep
            println!("[MockTcpStream::wait_for_pattern] No pattern found, sleeping with current data: {:?}", String::from_utf8_lossy(&data));
            
            drop(data);
            thread::sleep(Duration::from_millis(50));
        }
    }

    pub fn clear_written_data(&self) {
        self.write_data.lock().unwrap().clear();
    }

    pub fn clear_read_data(&self) {
        self.read_data.lock().unwrap().clear();
    }

    pub fn get_written_data(&self) -> Vec<u8> {
        self.write_data.lock().unwrap().clone()
    }

    pub fn shutdown(&self) {
        *self.shutdown.lock().unwrap() = true;
    }
}

impl Read for MockTcpStream {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let start_time = Instant::now();
        loop {
            if start_time.elapsed() > Duration::from_secs(2) {
                #[cfg(debug_assertions)]
                println!("[MockTcpStream::read] Timeout after 2s");
                return Ok(0);
            }

            if *self.shutdown.lock().unwrap() {
                return Ok(0);
            }

            let mut data = self.read_data.lock().unwrap();
            if !data.is_empty() {
                let n = std::cmp::min(buf.len(), data.len());
                buf[..n].copy_from_slice(&data[..n]);
                data.drain(..n);
                return Ok(n);
            }
            drop(data);
            thread::sleep(Duration::from_millis(50));
        }
    }
}

impl Write for MockTcpStream {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        if *self.shutdown.lock().unwrap() {
            return Ok(0); // Just ignore writes after shutdown
        }
        self.write_data.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

impl redis_starter_rust::redis::replication::TcpStreamTrait for MockTcpStream {
    fn peer_addr(&self) -> Result<std::net::SocketAddr> {
        Ok("127.0.0.1:6379".parse().unwrap())
    }

    fn try_clone(&self) -> Result<Box<dyn redis_starter_rust::redis::replication::TcpStreamTrait>> {
        Ok(Box::new(self.clone()))
    }
}
