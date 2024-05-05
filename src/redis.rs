use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};
use std::thread;
use std::io::{Read, Write};
use std::net::TcpStream;
use base64;
use base64::Engine;
use base64::engine::general_purpose;

fn gen_replid() -> String {
    // let mut rng = rand::thread_rng();
    // std::iter::repeat(())
    //     .map(|()| {
    //         let num = rng.gen_range(0..62);
    //         match num {
    //             0..=9 => (num + 48) as u8 as char, // 0-9
    //             10..=35 => (num + 87) as u8 as char, // a-z
    //             _ => (num + 29) as u8 as char, // A-Z
    //         }
    //     })
    //     .take(40)
    //     .collect()
    "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string()
}
#[derive(Debug)]
pub enum RedisCommand<'a> {
    None,
    Echo { data: &'a str },
    Ping,
    Set { key: &'a str, value: &'a str, ttl: Option<usize> },
    Get { key: &'a str },
    Info { subcommand: String },
    // I dont really like Vec here, but I think Replconf is not
    // oftenly invoked so I will leave it like this for now.
    Replconf { subcommand: &'a str, params: Vec<&'a str> },
    Psync { replica_id: &'a str, offset: i8 },
    Error { message: String },
}

impl RedisCommand<'_> {
    const PING : &'static str = "PING";
    const ECHO : &'static str = "ECHO";
    const SET : &'static str = "SET";
    const GET : &'static str = "GET";
    const INFO : &'static str = "INFO";
    const REPLCONF : &'static str = "REPLCONF";
    const PSYNC : &'static str = "PSYNC";

    pub fn is_none(&self) -> bool {
        match self {
            RedisCommand::None => true,
            _ => false,
        }
    }

    /// Create command from the data received from the client.
    /// It should check if the parameters are complete, otherwise return None.
    /// For example Set requires 2 parameters, key and value. When this method is called for
    /// a Set command, the first time it will return true to indicate that it expects another.
    pub fn data<'a>(command: &'a str, params: [&'a str;5]) -> Option<RedisCommand<'a>> {
        match command {
            command if command.eq_ignore_ascii_case(Self::PING) => Some(RedisCommand::Ping),
            command if command.eq_ignore_ascii_case(Self::ECHO) => {
                if params[0] == "" {
                    None
                } else {
                    Some(RedisCommand::Echo { data: params[0] })
                }
            },
            command if command.eq_ignore_ascii_case(Self::SET) => {
                let key = params[0];
                let value = params[1];
                if key == "" || value == "" {
                    None
                } else {
                    let ttl = match params[2].eq_ignore_ascii_case("EX") {
                        true => match params[3].parse::<usize>() {
                            Ok(value) => Some(value * 1000),
                            Err(_) => None,
                        },
                        false => match params[2].eq_ignore_ascii_case("PX") {
                            true => match params[3].parse::<usize>() {
                                Ok(value) => Some(value),
                                Err(_) => None,
                            },
                            false => None,
                        },
                    };
                    Some(RedisCommand::Set { key, value, ttl })
                }
            },
            command if command.eq_ignore_ascii_case(Self::GET) => {
                let key = params[0];
                if key == "" {
                    None
                } else {
                    Some(RedisCommand::Get { key })
                }
            },
            command if command.eq_ignore_ascii_case(Self::INFO) => {
                if params[0].is_empty() {
                    None
                } else {
                    Some(RedisCommand::Info { subcommand: params[0].to_string() })
                }
            },
            command if command.eq_ignore_ascii_case(Self::REPLCONF) => {
                let subcommand = params[0];
                let params = params[1..].iter().filter(|&&p| !p.is_empty()).cloned().collect();
                if subcommand == "" {
                    Some(RedisCommand::Error { message: "ERR Wrong number of arguments for 'replconf' command".to_string() })
                } else {
                    Some(RedisCommand::Replconf { subcommand, params })
                }
            },
            command if command.eq_ignore_ascii_case(Self::PSYNC) => {
                let replica_id = params[0];
                match params[1].parse::<i8>() {
                    Ok(offset) => Some(RedisCommand::Psync { replica_id, offset }),
                    Err(_) => Some(RedisCommand::Error { message: "ERR Invalid offset".to_string() }),
                }
            },
            _ => Some(RedisCommand::Error { message: format!("Unknown command: {}", command) }),
        }
    }
}

struct ValueWrapper {
    value: String,
    expiration: Option<u128>,
}

#[derive(Debug)]
#[derive(Clone)]
pub struct RedisConfig {
    pub addr: String,
    pub port: String,
    pub replicaof_host: Option<String>,
    pub replicaof_port: Option<String>,
}

impl RedisConfig {
    pub fn new() -> Self {
        RedisConfig {
            addr: "0.0.0.0".to_string(),
            port: "6379".to_string(),
            replicaof_host: None,
            replicaof_port: None,
        }
    }
}
pub struct Redis {
    config: RedisConfig,
    data: Mutex<HashMap<String, ValueWrapper>>,
}
impl Redis {
    pub fn new(config:RedisConfig) -> Self {
        Redis {
            config,
            data: Mutex::new(HashMap::new()),
        }
    }

    fn new_default() -> Self {
        Redis {
            config: RedisConfig::new(),
            data: Mutex::new(HashMap::new()),
        }
    }

    pub fn set(&mut self, key: &str, value: &str, ttl: Option<usize>) {
        let expiration =
            match ttl {
                Some(ttl) => {
                    if ttl == 0 {
                        None
                    } else {
                        Some(SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_millis() + ttl as u128)
                    }
                },
                None => None,
            };
        let value_with_ttl = ValueWrapper { value: value.to_string(), expiration: expiration };
        self.data.lock().unwrap().insert(key.to_string(), value_with_ttl);
    }

    pub fn get(&self, key: &str) -> Option<String> {
        let mut binding = self.data.lock().unwrap();
        let value_wrapper = binding.get(key);
        match value_wrapper {
            Some(value_with_ttl) => {
                match value_with_ttl.expiration {
                    Some(expiration) => {
                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_millis();
                        if now > expiration {
                            binding.remove(key);
                            None
                        } else {
                            Some(value_with_ttl.value.clone())
                        }
                    },
                    None => Some(value_with_ttl.value.clone()),
                }
            },
            None => None,
        }
    }

    // TODO client should be a TcpStream not an option.. find how to mock it in tests.
    pub fn execute_command(&mut self, command: &RedisCommand, client: Option<&mut TcpStream>) -> Result<String, String> {
        match command {
            RedisCommand::Ping => {
                Ok("+PONG\r\n".to_string())
            },
            RedisCommand::Echo { data } => {
                Ok(format!("${}\r\n{}\r\n", data.len(), data))
            },
            RedisCommand::Get { key } => {
                match self.get(key) {
                    Some(value) => Ok(format!("${}\r\n{}\r\n", value.len(), value)),
                    None => Ok("$-1\r\n".to_string()),
                }
            },
            RedisCommand::Set { key, value, ttl } => {
                self.set(key, value, *ttl);
                Ok("+OK\r\n".to_string())
            },
            RedisCommand::Info { subcommand } => {
                match subcommand.as_str() {
                    "replication" => {
                        let ret:String =
                            if self.config.replicaof_host.is_some() {
                                format!("role:slave\r\nmaster_replid:{}\r\nmaster_repl_offset:0\r\nmaster_host:{}\r\nmaster_port:{}",
                                    gen_replid(),
                                    self.config.replicaof_host.as_ref().unwrap(),
                                    self.config.replicaof_port.as_ref().unwrap())
                            } else {
                                format!("role:master\r\nmaster_replid:{}\r\nmaster_repl_offset:0\r\nconnected_slaves:0",
                                    gen_replid())
                            };
                        Ok(format!("${}\r\n{}\r\n", ret.len(), ret))
                    },
                    _ => Err("ERR Unknown INFO subcommand".to_string()),
                }
            },
            RedisCommand::Replconf {
                subcommand,params,
            } => {
                match subcommand {
                    &"listening-port" | &"capa" => {
                        // TODO: Implement the actual logic for these subcommands
                        Ok("+OK\r\n".to_string())
                    }
                    _ => Err(format!("ERR Unknown REPLCONF subcommand: {}\r\n", subcommand)),
                }
            },
            RedisCommand::Psync {
                replica_id, offset,
            } => {
                if *offset == -1 && *replica_id == "?" {
                    if let Some(client) = client {
                        client.write(format!("+FULLRESYNC {} {}\r\n", gen_replid(), 0).as_bytes());

                        let rdb_file_base64 = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";
                        // Decode the base64 string into a byte array
                        let rdb_file = general_purpose::STANDARD.decode(rdb_file_base64).unwrap();
                        // let length = rdb_file.len();
                        // let contents = String::from_utf8_lossy(&rdb_file);
                        let length = rdb_file.len();
                        println!("contents: {:?}", rdb_file.as_slice());
                        println!("contents length: {}", length);
                        client.write(format!("${}\r\n", length).as_bytes());
                        client.write(rdb_file.as_slice());
                        client.flush();
                    }

                    Ok("".to_string())
                    // Ok(format!(${}\r\n{}", gen_replid(), 0, rdb_file.len(), contents))
                } else {
                    Err("ERR Unknown PSYNC subcommand".to_string())
                }
            },
            RedisCommand::Error { message } => {
                Err(format!("ERR {}", message))
            },
            _ => Err("ERR Unknown command".to_string()),
        }
    }
}

#[inline]
fn connect_to_server(host: &str, port: &str) -> std::io::Result<std::net::TcpStream> {
    let replicaof_addr = format!("{}:{}", host, port);
    std::net::TcpStream::connect(replicaof_addr)
}

#[inline]
fn send_command(stream: &mut std::net::TcpStream, command: &str) -> std::io::Result<usize> {
    stream.write(command.as_bytes())
}

#[inline]
fn read_response(stream: &mut std::net::TcpStream, buffer: &mut [u8; 512]) -> std::io::Result<String> {
    let bytes_read = stream.read(buffer)?;
    Ok(std::str::from_utf8(&buffer[0..bytes_read]).unwrap().to_string())
}

#[inline]
pub fn init_replica(config: &mut RedisConfig) {
    // as part of startup sequence..
    // if there is a replicaof host and port then this instance is a replica.

    // for now let's just code it here.
    // connect to remote host and port, send PING as RESP.
    // (a) sends PING command - wait for +PONG
    // (b) sends REPLCONF listening-port <PORT> - wait for +OK
    // (c) sends REPLCONF capa eof capa psync2 - wait for +OK
    // (d) sends PSYNC ? -1 - wait for +fullresync
    // the following sequence in separate thread..
    let config = config.clone();
    let handle = thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_secs(1));
        if let Some(replicaof_host) = &config.replicaof_host {
            if let Some(replicaof_port) = &config.replicaof_port {
                match connect_to_server(replicaof_host, replicaof_port) {
                    Ok(mut stream) => {
                        let replconf_command = format!("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${}\r\n{}\r\n", config.port.len(), config.port);
                        let commands: Vec<(&str, Box<dyn Fn(&str) -> bool>)> = vec![
                            ("*1\r\n$4\r\nPING\r\n", Box::new(|response: &str| response == "+PONG\r\n")),
                            (&replconf_command, Box::new(|response: &str| response == "+OK\r\n")),
                            ("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n", Box::new(|response: &str| response == "+OK\r\n")),
                            ("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n", Box::new(|response: &str| response.to_lowercase().starts_with("+fullresync")))
                        ];

                        let mut buffer = [0; 512];
                        for (command, validate) in commands {
                            println!("sending command: {}", command);
                            if send_command(&mut stream, command).is_err() {
                                eprintln!("error executing command: {}", command);
                                std::process::exit(1);
                            }
                            println!("reading response..");
                            let response = read_response(&mut stream, &mut buffer).unwrap();
                            println!("response: {}", response);
                            if !validate(&response) {
                                eprintln!("unexpected response: {}", response);
                                std::process::exit(1);
                            }
                        }
                        println!("replicaof host and port is valid");
                    }
                    Err(e) => {
                        eprintln!("error connecting to replicaof host and port: {}", e);
                        std::process::exit(1);
                    }
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let db = Redis::new_default();
        assert!(db.data.lock().unwrap().is_empty());
    }

    #[test]
    fn test_set() {
        let mut db = Redis::new_default();
        db.set("key1", "value1", None);
        assert_eq!(db.get("key1"), Some("value1".to_string()));
    }

    #[test]
    fn test_get() {
        let mut db = Redis::new_default();
        db.set("key1", "value1", None);
        assert_eq!(db.get("key1"), Some("value1".to_string()));
    }

    #[test]
    fn test_set_ttl_and_get() {
        let mut db = Redis::new_default();
        db.set("key1", "value1", Some(1000));
        assert_eq!(db.get("key1"), Some("value1".to_string()));
        // sleep for 1 second
        std::thread::sleep(std::time::Duration::from_millis(1500));
        assert_eq!(db.get("key1"), None);
    }

    #[test]
    fn test_get_nonexistent() {
        let db = Redis::new_default();
        assert_eq!(db.get("key1"), None);
    }

    /// Test create RedisCommand::Set with TTL in milliseconds.
    /// SET key value PX 1000
    #[test]
    fn test_set_ttl_px() {
        let command = RedisCommand::data("SET", ["key", "value", "PX", "1000", ""]);
        match command {
            Some(RedisCommand::Set { key, value, ttl }) => {
                assert_eq!(key, "key");
                assert_eq!(value, "value");
                assert_eq!(ttl, Some(1000));
            },
            _ => panic!("Expected RedisCommand::Set"),
        }

    }

    /// Test create RedisCommand::Set with TTL in seconds.
    /// SET key value EX 1
    #[test]
    fn test_set_ttl_ex() {
        let command = RedisCommand::data("SET", ["key", "value", "EX", "1", ""]);
        match command {
            Some(RedisCommand::Set { key, value, ttl }) => {
                assert_eq!(key, "key");
                assert_eq!(value, "value");
                assert_eq!(ttl, Some(1000));
            },
            _ => panic!("Expected RedisCommand::Set"),
        }
    }

    #[test]
    fn test_replconf() {
        let mut db = Redis::new_default();
        let replconf_command = RedisCommand::Replconf {
            subcommand: "listening-port",
            params: vec!["6379"]
        };
        // TODO mock TcpStream...
        let result = db.execute_command(&replconf_command, None);
        assert_eq!(result, Ok("+OK\r\n".to_string()));

        let replconf_command = RedisCommand::Replconf {
            subcommand: "capa",
            params: vec!["psync2"]
        };
        let result = db.execute_command(&replconf_command, None);
        assert_eq!(result, Ok("+OK\r\n".to_string()));

        let replconf_command = RedisCommand::Replconf {
            subcommand: "unknown",
            params: vec!["param1", "param2"]
        };
        let result = db.execute_command(&replconf_command, None);
        assert_eq!(result, Err("ERR Unknown REPLCONF subcommand: unknown\r\n".to_string()));
    }

    #[test]
    fn test_psync_fullresync() {
        let mut db = Redis::new_default();
        let psync_command = RedisCommand::Psync {
            replica_id: "?",
            offset: -1,
        };
        let result = db.execute_command(&psync_command, None);
        assert!(result.is_ok());
        let response = result.unwrap();
        assert!(response.eq("")); // empty response, we write directly to the client.
    }
}