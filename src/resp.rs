use bytes::BytesMut;
use anyhow::{Result, Error};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    SimpleString(String),
    BulkString(String),
    Array(Vec<Value>),
    Null,
}

impl Value {
    pub fn serialize(self) -> String {
        match self {
            Value::SimpleString(s) => format!("+{}\r\n", s),
            Value::BulkString(s) => format!("${}\r\n{}\r\n", s.len(), s),
            Value::Null => "$-1\r\n".to_string(),
            _ => panic!("Unsupported value for serialization"),
        }
    }
}

pub struct RespHandler {
    stream: TcpStream,
    buffer: BytesMut,
}

impl RespHandler {
    pub fn new(stream: TcpStream) -> Self {
        RespHandler {
            stream,
            buffer: BytesMut::with_capacity(512),
        }
    }

    pub async fn read_value(&mut self) -> Result<Option<Value>> {
        loop {
            let bytes_read = self.stream.read_buf(&mut self.buffer).await?;
            if bytes_read == 0 {
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err(Error::msg("Connection closed abruptly"));
                }
            }

            if let Ok((v, _)) = parse_message(&self.buffer) {
                self.buffer.clear(); // For now, assume one command per read or clear after read. 
                // TODO: Advance buffer properly instead of clearing
                return Ok(Some(v));
            }
            // If parse failed (incomplete), continue reading
        }
    }

    pub async fn write_value(&mut self, value: Value) -> Result<()> {
        self.stream.write_all(value.serialize().as_bytes()).await?;
        Ok(())
    }
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
        let s = String::from_utf8(line.to_vec())?;
        return Ok((Value::SimpleString(s), i + 2));
    }
    Err(Error::msg("Incomplete simple string"))
}

fn parse_array(buffer: &[u8]) -> Result<(Value, usize)> {
    let (len, mut offset) = parse_integer(buffer)?;
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
        let s = String::from_utf8(buffer[start..end].to_vec())?;
        return Ok((Value::BulkString(s), end + 2));
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
        assert_eq!(value, Value::SimpleString("OK".to_string()));
        assert_eq!(consumed, 5);
    }

    #[test]
    fn test_parse_bulk_string() {
        let buffer = b"$5\r\nhello\r\n";
        let (value, consumed) = parse_message(buffer).unwrap();
        assert_eq!(value, Value::BulkString("hello".to_string()));
        assert_eq!(consumed, 11);
    }

    #[test]
    fn test_parse_array() {
        let buffer = b"*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n";
        let (value, consumed) = parse_message(buffer).unwrap();
        if let Value::Array(items) = value {
            assert_eq!(items.len(), 2);
            assert_eq!(items[0], Value::BulkString("ECHO".to_string()));
            assert_eq!(items[1], Value::BulkString("hello".to_string()));
        } else {
            panic!("Expected Array");
        }
        assert_eq!(consumed, 25);
    }

    #[test]
    fn test_serialize_simple_string() {
        let val = Value::SimpleString("OK".to_string());
        assert_eq!(val.serialize(), "+OK\r\n");
    }

    #[test]
    fn test_serialize_bulk_string() {
        let val = Value::BulkString("hello".to_string());
        assert_eq!(val.serialize(), "$5\r\nhello\r\n");
    }
    
    #[test]
    fn test_serialize_null() {
        let val = Value::Null;
        assert_eq!(val.serialize(), "$-1\r\n");
    }
}
