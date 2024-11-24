use std::io::{Read, Write, Result};
use std::sync::{Arc, Mutex};
use std::net::SocketAddr;
use std::thread;
use std::time::{Duration, Instant};
use redis_starter_rust::redis::replication::TcpStreamTrait;

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

    pub fn get_written_data(&self) -> Vec<u8> {
        let data = self.write_data.lock().unwrap().clone();
        #[cfg(debug_assertions)]
        println!("[MockTcpStream::get_written_data] Current written data (len={}): {:?}", 
            data.len(), String::from_utf8_lossy(&data));
        data
    }

    pub fn clear_written_data(&self) {
        #[cfg(debug_assertions)]
        println!("[MockTcpStream::clear_written_data] Clearing write buffer");
        self.write_data.lock().unwrap().clear();
    }

    pub fn clear_read_data(&self) {
        #[cfg(debug_assertions)]
        println!("[MockTcpStream::clear_read_data] Clearing read buffer");
        self.read_data.lock().unwrap().clear();
    }

    pub fn shutdown(&self) {
        #[cfg(debug_assertions)]
        println!("[MockTcpStream::shutdown] Setting shutdown flag");
        *self.shutdown.lock().unwrap() = true;
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

impl TcpStreamTrait for MockTcpStream {
    fn peer_addr(&self) -> Result<SocketAddr> {
        Ok("127.0.0.1:8080".parse().unwrap())
    }

    fn try_clone(&self) -> Result<Box<dyn TcpStreamTrait>> {
        Ok(Box::new(self.clone()))
    }
}
