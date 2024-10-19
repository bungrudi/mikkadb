use std::path::Path;
use std::fs::File;
use std::io::{Read, BufReader, Error, ErrorKind};

pub struct RdbParser;

impl RdbParser {
    pub fn parse(path: &Path) -> std::io::Result<Vec<(String, String, Option<u64>)>> {
        println!("Attempting to open RDB file: {:?}", path);
        if !path.exists() {
            println!("RDB file does not exist");
            return Ok(vec![]);
        }

        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer)?;

        println!("RDB file size: {} bytes", buffer.len());

        if buffer.len() < 9 || &buffer[0..5] != b"REDIS" {
            println!("Invalid RDB file format");
            return Err(Error::new(ErrorKind::InvalidData, "Invalid RDB file format"));
        }

        let mut pos = 9; // Skip the header
        let mut result = Vec::new();

        while pos < buffer.len() {
            println!("Parsing at position: {}", pos);
            match buffer[pos] {
                0xFA => {
                    // Auxiliary field, skip it
                    println!("Skipping auxiliary field");
                    pos += 1;
                    let (_, new_pos) = Self::parse_string(&buffer, pos)?;
                    pos = new_pos;
                    let (_, new_pos) = Self::parse_string(&buffer, pos)?;
                    pos = new_pos;
                }
                0xFE => {
                    println!("Database selector found");
                    // Database selector
                    pos += 1;
                    // Skip database number and hash table sizes
                    while buffer[pos] != 0xFB && pos < buffer.len() {
                        pos += 1;
                    }
                    pos += 3;
                }
                0xFF => {
                    println!("End of file marker found");
                    // End of file
                    break;
                }
                _ => {
                    println!("Parsing key-value pair");
                    // Key-value pair
                    match Self::parse_key_value(&buffer, pos) {
                        Ok((key, value, expiry, new_pos)) => {
                            println!("Parsed key: {} with expiry: {:?}", key, expiry);
                            result.push((key, value, expiry));
                            pos = new_pos;
                        }
                        Err(e) => {
                            println!("Error parsing key-value pair: {:?}", e);
                            return Err(e);
                        }
                    }
                }
            }
        }

        println!("RDB file parsing completed");
        Ok(result)
    }

    fn parse_key_value(buffer: &[u8], mut pos: usize) -> std::io::Result<(String, String, Option<u64>, usize)> {
        let mut expiry = None;

        // Check for expiry
        if buffer[pos] == 0xFD || buffer[pos] == 0xFC {
            let expiry_bytes = if buffer[pos] == 0xFD { 4 } else { 8 };
            pos += 1;
            let mut expiry_value = 0u64;
            for i in 0..expiry_bytes {
                expiry_value |= (buffer[pos + i] as u64) << (8 * i);
            }
            expiry = Some(expiry_value);
            pos += expiry_bytes;
        }

        // Skip value type
        pos += 1;

        // Parse key
        let (key, new_pos) = Self::parse_string(buffer, pos)?;
        pos = new_pos;

        // Parse value
        let (value, new_pos) = Self::parse_string(buffer, pos)?;
        pos = new_pos;

        Ok((key, value, expiry, pos))
    }

    fn parse_string(buffer: &[u8], mut pos: usize) -> std::io::Result<(String, usize)> {
        let len = match buffer[pos] >> 6 {
            0 => {
                let len = (buffer[pos] & 0x3F) as usize;
                pos += 1;
                len
            }
            1 => {
                let len = (((buffer[pos] & 0x3F) as usize) << 8) | buffer[pos + 1] as usize;
                pos += 2;
                len
            }
            2 => {
                let len = ((buffer[pos + 1] as usize) << 24)
                    | ((buffer[pos + 2] as usize) << 16)
                    | ((buffer[pos + 3] as usize) << 8)
                    | (buffer[pos + 4] as usize);
                pos += 5;
                len
            }
            3 => {
                // Special encoding
                match buffer[pos] & 0x3F {
                    0 => {
                        // 8 bit integer
                        let value = buffer[pos + 1] as i8;
                        pos += 2;
                        return Ok((value.to_string(), pos));
                    }
                    1 => {
                        // 16 bit integer
                        let value = i16::from_le_bytes([buffer[pos + 1], buffer[pos + 2]]);
                        pos += 3;
                        return Ok((value.to_string(), pos));
                    }
                    2 => {
                        // 32 bit integer
                        let value = i32::from_le_bytes([buffer[pos + 1], buffer[pos + 2], buffer[pos + 3], buffer[pos + 4]]);
                        pos += 5;
                        return Ok((value.to_string(), pos));
                    }
                    _ => {
                        return Err(Error::new(ErrorKind::InvalidData, "Unsupported string encoding"));
                    }
                }
            }
            _ => {
                return Err(Error::new(ErrorKind::InvalidData, "Invalid string encoding"));
            }
        };

        if pos + len > buffer.len() {
            return Err(Error::new(ErrorKind::InvalidData, "String length exceeds buffer size"));
        }

        let s = String::from_utf8_lossy(&buffer[pos..pos + len]).to_string();
        pos += len;

        Ok((s, pos))
    }
}
