use std::io::{Read, Write, Result};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use redis_starter_rust::client_handler::ClientHandler;

#[derive(Clone)]
pub struct MockTcpStream {
    pub read_data: Arc<Mutex<Vec<u8>>>,
    pub write_data: Arc<Mutex<Vec<u8>>>,
    pub shutdown: Arc<Mutex<bool>>,
}

impl MockTcpStream {
    pub fn new() -> Self {
        MockTcpStream {
            read_data: Arc::new(Mutex::new(Vec::new())),
            write_data: Arc::new(Mutex::new(Vec::new())),
            shutdown: Arc::new(Mutex::new(false)),
        }
    }

    #[allow(dead_code)]
    pub fn get_written_data(&self) -> Vec<u8> {
        let data = self.write_data.lock().unwrap().clone();
        #[cfg(debug_assertions)]
        println!("[MockTcpStream::get_written_data] Current written data (len={}): {:?}", 
            data.len(), String::from_utf8_lossy(&data));
        data
    }

    #[allow(dead_code)]
    pub fn clear_written_data(&self) {
        #[cfg(debug_assertions)]
        println!("[MockTcpStream::clear_written_data] Clearing write buffer");
        self.write_data.lock().unwrap().clear();
    }

    #[allow(dead_code)]
    pub fn clear_read_data(&self) {
        let mut read_data = self.read_data.lock().unwrap();
        read_data.clear();
    }

    /// Shutdown the mock TCP stream and associated client handler
    pub fn shutdown(&self, client_handler: &mut ClientHandler, handle: JoinHandle<()>) {
        // Set shutdown flag
        {
            let mut shutdown = self.shutdown.lock().unwrap();
            *shutdown = true;
        }

        // Shutdown client handler
        client_handler.shutdown();

        // Give time for shutdown to propagate and wait for handle
        thread::sleep(Duration::from_millis(100));
        handle.join().unwrap();
    }

    /// Wait for a specific pattern to be written to the stream
    pub fn wait_for_write(&self, pattern: &str, timeout_ms: u64) -> bool {
        let start_time = Instant::now();
        let pattern_bytes = pattern.as_bytes();
        
        loop {
            // Check timeout
            if start_time.elapsed() > Duration::from_millis(timeout_ms) {
                #[cfg(debug_assertions)]
                println!("[MockTcpStream::wait_for_write] Timeout after {}ms", timeout_ms);
                return false;
            }

            // Check shutdown flag
            if *self.shutdown.lock().unwrap() {
                #[cfg(debug_assertions)]
                println!("[MockTcpStream::wait_for_write] Stream is shutdown");
                return false;
            }

            // Try to find pattern in written data
            let data = self.write_data.lock().unwrap();
            if data.windows(pattern_bytes.len()).any(|window| window == pattern_bytes) {
                #[cfg(debug_assertions)]
                println!("[MockTcpStream::wait_for_write] Pattern found");
                return true;
            }
            
            // No pattern found, release lock before sleeping
            drop(data);
            
            #[cfg(debug_assertions)]
            println!("[MockTcpStream::wait_for_write] No pattern found, sleeping");
            thread::sleep(Duration::from_millis(50));
        }
    }
}

impl Read for MockTcpStream {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let start_time = Instant::now();
        loop {
            // Check timeout
            if start_time.elapsed() > Duration::from_secs(2) {
                #[cfg(debug_assertions)]
                println!("[MockTcpStream::read] Timeout after 2s, setting shutdown flag");
                *self.shutdown.lock().unwrap() = true;
                return Ok(0);
            }

            // Check shutdown flag
            if *self.shutdown.lock().unwrap() {
                #[cfg(debug_assertions)]
                println!("[MockTcpStream::read] Stream is shutdown, returning 0");
                return Ok(0);
            }

            // Try to read data
            let mut data = self.read_data.lock().unwrap();
            let n = std::cmp::min(buf.len(), data.len());
            if n > 0 {
                buf[..n].copy_from_slice(&data[..n]);
                *data = data[n..].to_vec();
                #[cfg(debug_assertions)]
                println!("[MockTcpStream::read] Read {} bytes, remaining data len: {}", n, data.len());
                return Ok(n);
            }
            
            // No data available, release lock before sleeping
            drop(data);
            
            #[cfg(debug_assertions)]
            println!("[MockTcpStream::read] No data available, sleeping");
            thread::sleep(Duration::from_millis(50));
        }
    }
}

impl Write for MockTcpStream {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        if *self.shutdown.lock().unwrap() {
            #[cfg(debug_assertions)]
            println!("[MockTcpStream::write] Stream is shutdown, returning error");
            return Err(std::io::Error::new(std::io::ErrorKind::BrokenPipe, "Stream is shutdown"));
        }
        #[cfg(debug_assertions)]
        println!("[MockTcpStream::write] Writing {} bytes: {:?}", 
            buf.len(), String::from_utf8_lossy(buf));
        self.write_data.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> Result<()> {
        #[cfg(debug_assertions)]
        println!("[MockTcpStream::flush] Flushing stream");
        Ok(())
    }
}

impl redis_starter_rust::redis::replication::TcpStreamTrait for MockTcpStream {
    fn peer_addr(&self) -> Result<std::net::SocketAddr> {
        Ok("127.0.0.1:8080".parse().unwrap())
    }

    fn try_clone(&self) -> Result<Box<dyn redis_starter_rust::redis::replication::TcpStreamTrait>> {
        Ok(Box::new(self.clone()))
    }
}
