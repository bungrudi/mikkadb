use std::io::{Read, Write, Result};
use std::sync::{Arc, Mutex};
use std::net::SocketAddr;
use redis_starter_rust::redis::replication::TcpStreamTrait;

#[derive(Clone)]
pub struct MockTcpStream {
    pub read_data: Arc<Mutex<Vec<u8>>>,
    pub write_data: Arc<Mutex<Vec<u8>>>,
}

impl MockTcpStream {
    pub fn new() -> Self {
        MockTcpStream {
            read_data: Arc::new(Mutex::new(Vec::new())),
            write_data: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn get_written_data(&self) -> Vec<u8> {
        self.write_data.lock().unwrap().clone()
    }
}

impl Read for MockTcpStream {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let mut data = self.read_data.lock().unwrap();
        let n = std::cmp::min(buf.len(), data.len());
        buf[..n].copy_from_slice(&data[..n]);
        *data = data[n..].to_vec();
        Ok(n)
    }
}

impl Write for MockTcpStream {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        self.write_data.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> Result<()> {
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
