use bytes::{Bytes, BytesMut};
use anyhow::{Result, Error};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use std::time::{Duration, Instant};

/// Buffer tier for adaptive buffering strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BufferTier {
    /// 512 bytes - optimized for single commands, low latency
    Small,
    /// 4KB - moderate pipelining (2-8 commands per read)
    Medium,
    /// 16KB - heavy pipelining (8+ commands per read)
    Large,
}

impl BufferTier {
    fn capacity(&self) -> usize {
        match self {
            BufferTier::Small => 512,
            BufferTier::Medium => 4 * 1024,
            BufferTier::Large => 16 * 1024,
        }
    }
}

/// Result of attempting to parse RESP value from buffer
#[derive(Debug)]
pub enum ParseResult {
    /// Successfully parsed a complete value (value, bytes_consumed)
    Complete(Value, usize),
    /// Buffer contains incomplete data, need more bytes
    Incomplete,
    /// Parsing error (malformed RESP)
    Error(anyhow::Error),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    SimpleString(Bytes),
    BulkString(Bytes),
    Array(Vec<Value>),
    Integer(i64),
    RdbFile(Vec<u8>),
    Multiple(Vec<Value>),
    Error(Bytes),
    Null,
    NullArray,
}

impl Value {
    /// Convert Value to String (adapter layer for storage boundary)
    /// This is where UTF-8 validation happens - commands must be valid UTF-8
    pub fn to_string(&self) -> Result<String> {
        match self {
            Value::SimpleString(b) => Ok(std::str::from_utf8(b)
                .map_err(|_| Error::msg("Invalid UTF-8 in SimpleString"))?
                .to_string()),
            Value::BulkString(b) => Ok(std::str::from_utf8(b)
                .map_err(|_| Error::msg("Invalid UTF-8 in BulkString"))?
                .to_string()),
            Value::Error(b) => Ok(std::str::from_utf8(b)
                .map_err(|_| Error::msg("Invalid UTF-8 in Error"))?
                .to_string()),
            _ => Err(Error::msg("Cannot convert this Value type to String")),
        }
    }

    /// Extract bulk string as uppercase String (common for command names)
    pub fn to_uppercase_string(&self) -> Result<String> {
        match self {
            Value::BulkString(b) => {
                let s = std::str::from_utf8(b)
                    .map_err(|_| Error::msg("Invalid UTF-8 in command"))?;
                Ok(s.to_uppercase())
            }
            _ => Err(Error::msg("Expected BulkString for command")),
        }
    }

    #[cfg(test)]
    pub fn serialize(self) -> String {
        match self {
            Value::SimpleString(s) => format!("+{}\r\n", String::from_utf8_lossy(&s)),
            Value::BulkString(s) => format!("${}\r\n{}\r\n", s.len(), String::from_utf8_lossy(&s)),
            Value::Integer(i) => format!(":{}\r\n", i),
            Value::Array(items) => {
                let mut s = format!("*{}\r\n", items.len());
                for item in items {
                    s.push_str(&item.serialize());
                }
                s
            }
            Value::RdbFile(_) => panic!("Cannot serialize RdbFile to String"),
            Value::Multiple(_) => panic!("Cannot serialize Multiple to String"),
            Value::Error(s) => format!("-{}\r\n", String::from_utf8_lossy(&s)),
            Value::Null => "$-1\r\n".to_string(),
            Value::NullArray => "*-1\r\n".to_string(),
        }
    }
    
    pub fn serialize_bytes(self) -> Vec<u8> {
        match self {
            Value::SimpleString(s) => {
                let mut bytes = Vec::with_capacity(s.len() + 3);
                bytes.push(b'+');
                bytes.extend_from_slice(&s);
                bytes.extend_from_slice(b"\r\n");
                bytes
            }
            Value::BulkString(s) => {
                let len_str = s.len().to_string();
                let mut bytes = Vec::with_capacity(1 + len_str.len() + 2 + s.len() + 2);
                bytes.push(b'$');
                bytes.extend_from_slice(len_str.as_bytes());
                bytes.extend_from_slice(b"\r\n");
                bytes.extend_from_slice(&s);
                bytes.extend_from_slice(b"\r\n");
                bytes
            }
            Value::Integer(i) => format!(":{}\r\n", i).into_bytes(),
            Value::Array(items) => {
                let mut bytes = format!("*{}\r\n", items.len()).into_bytes();
                for item in items {
                    bytes.extend(item.serialize_bytes());
                }
                bytes
            }
            Value::RdbFile(data) => {
                let mut bytes = format!("${}\r\n", data.len()).into_bytes();
                bytes.extend(data);
                bytes
            }
            Value::Multiple(items) => {
                let mut bytes = Vec::new();
                for item in items {
                    bytes.extend(item.serialize_bytes());
                }
                bytes
            }
            Value::Error(s) => {
                let mut bytes = Vec::with_capacity(s.len() + 3);
                bytes.push(b'-');
                bytes.extend_from_slice(&s);
                bytes.extend_from_slice(b"\r\n");
                bytes
            }
            Value::Null => "$-1\r\n".to_string().into_bytes(),
            Value::NullArray => "*-1\r\n".to_string().into_bytes(),
        }
    }
}

pub struct RespHandler {
    reader: OwnedReadHalf,
    writer: BufWriter<OwnedWriteHalf>,
    buffer: BytesMut,
    tier: BufferTier,
    last_activity: Instant,
    commands_in_last_read: usize,
}

impl RespHandler {
    pub fn new(stream: TcpStream) -> Self {
        let (reader, writer) = stream.into_split();
        RespHandler {
            reader,
            writer: BufWriter::new(writer),
            // Start with small buffer (Tier0) for low-latency single commands
            // Will upgrade dynamically when pipelining is detected
            buffer: BytesMut::with_capacity(BufferTier::Small.capacity()),
            tier: BufferTier::Small,
            last_activity: Instant::now(),
            commands_in_last_read: 0,
        }
    }

    /// Check if connection has been idle and downgrade buffer tier if needed
    const IDLE_TIMEOUT: Duration = Duration::from_secs(2);

    fn check_idle_downgrade(&mut self) {
        if self.last_activity.elapsed() > Self::IDLE_TIMEOUT && self.tier != BufferTier::Small {
            // Connection idle - downgrade to small buffer for low latency
            self.tier = BufferTier::Small;
            // Keep the allocated capacity but logically reset to small tier
        }
    }

    /// Upgrade buffer tier based on number of commands parsed
    fn check_upgrade(&mut self, commands_parsed: usize) {
        match self.tier {
            BufferTier::Small if commands_parsed >= 2 => {
                // Pipelining detected - upgrade to medium
                self.tier = BufferTier::Medium;
                self.buffer.reserve(BufferTier::Medium.capacity() - self.buffer.capacity());
            }
            BufferTier::Medium if commands_parsed >= 8 => {
                // Heavy pipelining detected - upgrade to large
                self.tier = BufferTier::Large;
                self.buffer.reserve(BufferTier::Large.capacity() - self.buffer.capacity());
            }
            _ => {}
        }
    }

    pub async fn read_value(&mut self) -> Result<Option<Value>> {
        loop {
            // Check for idle timeout and downgrade if needed (before read)
            self.check_idle_downgrade();

            // Try to parse from existing buffer first
            if let Ok((v, consumed)) = parse_message(&self.buffer) {
                let _ = self.buffer.split_to(consumed);
                self.last_activity = Instant::now();

                // CONDITIONAL READ-AHEAD: Only for Medium and Large tiers
                // Small tier stays low-latency without aggressive buffering
                if self.tier != BufferTier::Small {
                    self.try_fill_buffer().await;
                }

                return Ok(Some(v));
            }

            // Need more data - blocking read
            let bytes_read = self.reader.read_buf(&mut self.buffer).await?;
            if bytes_read == 0 {
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err(Error::msg("Connection closed abruptly"));
                }
            }
        }
    }

    /// Aggressively read all available data from socket without blocking
    /// Only called for Medium and Large buffer tiers (conditional read-ahead)
    async fn try_fill_buffer(&mut self) {
        // Reserve space based on current tier
        let reserve_size = match self.tier {
            BufferTier::Small => return, // Should not be called for Small tier
            BufferTier::Medium => 4 * 1024,
            BufferTier::Large => 8 * 1024,
        };
        self.buffer.reserve(reserve_size);

        // Try to read multiple times to fill buffer with all available data
        let mut temp_buf = vec![0u8; 8192];
        loop {
            match self.reader.try_read(&mut temp_buf) {
                Ok(0) => break, // EOF
                Ok(n) => {
                    // Got data - append to buffer and try for more
                    self.buffer.extend_from_slice(&temp_buf[..n]);
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    // No more data available right now - that's fine
                    break;
                }
                Err(_) => break, // Other error - stop trying
            }
        }
    }

    /// Notify that a command batch was processed (for adaptive buffering)
    pub fn notify_batch_processed(&mut self, commands_parsed: usize) {
        self.commands_in_last_read = commands_parsed;
        self.check_upgrade(commands_parsed);
    }

    pub async fn write_value(&mut self, value: Value) -> Result<()> {
        self.writer.write_all(&value.serialize_bytes()).await?;
        self.writer.flush().await?;
        Ok(())
    }

    /// Try to parse a RESP value from the internal buffer without blocking I/O
    /// Returns Complete with value (buffer is automatically advanced), Incomplete if more data needed,
    /// or Error if the data is malformed
    pub fn try_read_value_from_buf(&mut self) -> ParseResult {
        match parse_message(&self.buffer) {
            Ok((value, consumed)) => {
                // Advance buffer past the consumed bytes
                let _ = self.buffer.split_to(consumed);
                ParseResult::Complete(value, consumed)
            }
            Err(e) => {
                // Check if error is due to incomplete data
                let msg = e.to_string();
                if msg.contains("Incomplete") || msg.contains("Empty buffer") {
                    ParseResult::Incomplete
                } else {
                    ParseResult::Error(e)
                }
            }
        }
    }

    /// Write multiple responses with a single flush operation
    /// This is the core of command pipelining optimization
    pub async fn write_batch(&mut self, responses: Vec<Value>) -> Result<()> {
        if responses.is_empty() {
            return Ok(());
        }

        // Write all responses to the buffer
        for response in responses {
            self.writer.write_all(&response.serialize_bytes()).await?;
        }

        // Single flush for entire batch
        self.writer.flush().await?;
        Ok(())
    }

    pub async fn read_rdb_file(&mut self) -> Result<Vec<u8>> {
        loop {
            if let Ok((data, consumed)) = parse_rdb_file(&self.buffer) {
                let _ = self.buffer.split_to(consumed);
                return Ok(data);
            }

            let bytes_read = self.reader.read_buf(&mut self.buffer).await?;
            if bytes_read == 0 {
                if self.buffer.is_empty() {
                    return Err(Error::msg("Connection closed abruptly"));
                } else {
                    return Err(Error::msg("Connection closed abruptly"));
                }
            }
        }
    }
}

fn parse_rdb_file(buffer: &[u8]) -> Result<(Vec<u8>, usize)> {
    if buffer.is_empty() {
        return Err(Error::msg("Empty buffer"));
    }
    if buffer[0] != b'$' {
        return Err(Error::msg("Expected $ for RDB file"));
    }
    
    let (len, header_len) = parse_integer(buffer)?;
    let len = len as usize;
    let total_len = header_len + len;
    
    if buffer.len() >= total_len {
        let data = buffer[header_len..total_len].to_vec();
        return Ok((data, total_len));
    }
    
    Err(Error::msg("Incomplete RDB file"))
}

fn parse_message(buffer: &[u8]) -> Result<(Value, usize)> {
    if buffer.is_empty() {
        return Err(Error::msg("Empty buffer"));
    }
    match buffer[0] as char {
        '+' => parse_simple_string(buffer),
        '*' => parse_array(buffer),
        '$' => parse_bulk_string(buffer),
        _ => Err(Error::msg("Unknown type")),
    }
}

fn parse_simple_string(buffer: &[u8]) -> Result<(Value, usize)> {
    if let Some(i) = buffer.windows(2).position(|w| w == b"\r\n") {
        let line = &buffer[1..i];
        let bytes = Bytes::copy_from_slice(line);
        return Ok((Value::SimpleString(bytes), i + 2));
    }
    Err(Error::msg("Incomplete simple string"))
}

fn parse_array(buffer: &[u8]) -> Result<(Value, usize)> {
    let (len, mut offset) = parse_integer(buffer)?;
    if len < 0 {
        // RESP null array: *-1\r\n
        return Ok((Value::NullArray, offset));
    }
    let mut items = Vec::new();
    
    for _ in 0..len {
        let (val, consumed) = parse_message(&buffer[offset..])?;
        items.push(val);
        offset += consumed;
    }
    
    Ok((Value::Array(items), offset))
}

fn parse_bulk_string(buffer: &[u8]) -> Result<(Value, usize)> {
    let (len, rem) = parse_integer(buffer)?;
    let start = rem;
    let end = start + len as usize;

    if end + 2 <= buffer.len() {
        let bytes = Bytes::copy_from_slice(&buffer[start..end]);
        return Ok((Value::BulkString(bytes), end + 2));
    }
    Err(Error::msg("Incomplete bulk string"))
}

fn parse_integer(buffer: &[u8]) -> Result<(i64, usize)> {
    if let Some(i) = buffer.windows(2).position(|w| w == b"\r\n") {
        let line = &buffer[1..i];
        let s = String::from_utf8(line.to_vec())?;
        let num = s.parse::<i64>()?;
        return Ok((num, i + 2));
    }
    Err(Error::msg("Incomplete integer"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_string() {
        let buffer = b"+OK\r\n";
        let (value, consumed) = parse_message(buffer).unwrap();
        assert_eq!(value, Value::SimpleString(Bytes::from("OK")));
        assert_eq!(consumed, 5);
    }

    #[test]
    fn test_parse_bulk_string() {
        let buffer = b"$5\r\nhello\r\n";
        let (value, consumed) = parse_message(buffer).unwrap();
        assert_eq!(value, Value::BulkString(Bytes::from("hello")));
        assert_eq!(consumed, 11);
    }

    #[test]
    fn test_parse_array() {
        let buffer = b"*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n";
        let (value, consumed) = parse_message(buffer).unwrap();
        if let Value::Array(items) = value {
            assert_eq!(items.len(), 2);
            assert_eq!(items[0], Value::BulkString(Bytes::from("ECHO")));
            assert_eq!(items[1], Value::BulkString(Bytes::from("hello")));
        } else {
            panic!("Expected Array");
        }
        assert_eq!(consumed, 25);
    }

    #[test]
    fn test_serialize_simple_string() {
        let val = Value::SimpleString(Bytes::from("OK"));
        assert_eq!(val.serialize(), "+OK\r\n");
    }

    #[test]
    fn test_serialize_bulk_string() {
        let val = Value::BulkString(Bytes::from("hello"));
        assert_eq!(val.serialize(), "$5\r\nhello\r\n");
    }
    
    #[test]
    fn test_serialize_null() {
        let val = Value::Null;
        assert_eq!(val.serialize(), "$-1\r\n");
    }

    #[test]
    fn test_serialize_null_array() {
        let val = Value::NullArray;
        assert_eq!(val.serialize(), "*-1\r\n");
    }

    #[test]
    fn test_serialize_array() {
        let val = Value::Array(vec![
            Value::SimpleString(Bytes::from("OK")),
            Value::BulkString(Bytes::from("hello")),
        ]);
        assert_eq!(val.serialize(), "*2\r\n+OK\r\n$5\r\nhello\r\n");
    }

    // Phase 1: New tests for try_read_value_from_buf()
    #[test]
    fn test_try_read_value_from_buf_empty() {
        // Test parse_message with empty buffer
        let buffer = b"";
        let result = parse_message(buffer);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Empty buffer"));
    }

    #[test]
    fn test_try_read_value_from_buf_incomplete_simple_string() {
        let buffer = b"+OK"; // Missing \r\n
        let result = parse_message(buffer);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Incomplete"));
    }

    #[test]
    fn test_try_read_value_from_buf_incomplete_bulk_string() {
        let buffer = b"$5\r\nhel"; // Incomplete data
        let result = parse_message(buffer);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Incomplete"));
    }

    #[test]
    fn test_try_read_value_from_buf_complete_with_extra() {
        // Buffer contains complete command plus extra data
        let buffer = b"$5\r\nhello\r\n+OK\r\n";
        let (value, consumed) = parse_message(buffer).unwrap();
        assert_eq!(value, Value::BulkString(Bytes::from("hello")));
        assert_eq!(consumed, 11);
        // Verify extra data is left in buffer
        assert_eq!(&buffer[consumed..], b"+OK\r\n");
    }

    #[test]
    fn test_try_read_value_from_buf_multiple_commands() {
        // Simulate parsing multiple commands from buffer
        let buffer = b"$3\r\nGET\r\n$3\r\nfoo\r\n$3\r\nSET\r\n$3\r\nbar\r\n$5\r\nvalue\r\n";
        let mut offset = 0;

        // Parse first command (GET foo)
        let (val1, consumed1) = parse_message(&buffer[offset..]).unwrap();
        assert_eq!(val1, Value::BulkString(Bytes::from("GET")));
        offset += consumed1;

        let (val2, consumed2) = parse_message(&buffer[offset..]).unwrap();
        assert_eq!(val2, Value::BulkString(Bytes::from("foo")));
        offset += consumed2;

        // Parse second command (SET bar value)
        let (val3, consumed3) = parse_message(&buffer[offset..]).unwrap();
        assert_eq!(val3, Value::BulkString(Bytes::from("SET")));
        offset += consumed3;

        let (val4, consumed4) = parse_message(&buffer[offset..]).unwrap();
        assert_eq!(val4, Value::BulkString(Bytes::from("bar")));
        offset += consumed4;

        let (val5, consumed5) = parse_message(&buffer[offset..]).unwrap();
        assert_eq!(val5, Value::BulkString(Bytes::from("value")));
        offset += consumed5;

        assert_eq!(offset, buffer.len());
    }

    #[test]
    fn test_serialize_batch() {
        // Test that batch serialization matches individual serialization
        let responses = vec![
            Value::SimpleString(Bytes::from("OK")),
            Value::BulkString(Bytes::from("value1")),
            Value::Integer(42),
            Value::Null,
        ];

        // Serialize individually
        let mut expected = Vec::new();
        for resp in &responses {
            expected.extend(resp.clone().serialize_bytes());
        }

        // Serialize as batch (manually, since write_batch is async)
        let mut actual = Vec::new();
        for resp in responses {
            actual.extend(resp.serialize_bytes());
        }

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_serialize_empty_batch() {
        let responses: Vec<Value> = vec![];
        let mut output = Vec::new();
        for resp in responses {
            output.extend(resp.serialize_bytes());
        }
        assert_eq!(output.len(), 0);
    }

    #[test]
    fn test_serialize_large_batch() {
        // Test batch of 100 responses
        let mut responses = Vec::new();
        for i in 0..100 {
            responses.push(Value::Integer(i));
        }

        let mut output = Vec::new();
        for resp in responses {
            output.extend(resp.serialize_bytes());
        }

        // Should have 100 integer responses
        assert!(output.len() > 100); // At least ":0\r\n" = 4 bytes per response
    }
}
