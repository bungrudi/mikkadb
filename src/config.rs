use std::env;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub enum ServerRole {
    Master,
    Slave,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub port: u16,
    pub role: ServerRole,
    pub master_replid: String,
    pub master_repl_offset: i64,
    pub master_host: Option<String>,
    pub master_port: Option<u16>,
    pub data_dir: String,
    pub db_filename: String,
    pub num_shards: usize,
}

impl Config {
    pub fn parse() -> Self {
        let args: Vec<String> = env::args().collect();
        let mut port = 6379;
        let mut role = ServerRole::Master;
        let mut master_host = None;
        let mut master_port = None;
        let mut data_dir = ".".to_string();
        let mut db_filename = "dump.rdb".to_string();
        let mut num_shards = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);

        let mut i = 1; // Start from 1 (skip program name)
        while i < args.len() {
            match args[i].as_str() {
                "--port" => {
                    if i + 1 < args.len() {
                        if let Ok(p) = args[i + 1].parse() {
                            port = p;
                        }
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--replicaof" => {
                    if i + 1 < args.len() {
                        let arg = &args[i+1];
                        if arg.contains(' ') {
                            // Handle single string argument: "host port"
                            let parts: Vec<&str> = arg.split_whitespace().collect();
                            if parts.len() >= 2 {
                                role = ServerRole::Slave;
                                master_host = Some(parts[0].to_string());
                                if let Ok(p) = parts[1].parse() {
                                    master_port = Some(p);
                                }
                            }
                            i += 2;
                        } else if i + 2 < args.len() {
                            // Handle two separate arguments: host port
                            role = ServerRole::Slave;
                            master_host = Some(args[i + 1].clone());
                            if let Ok(p) = args[i + 2].parse() {
                                master_port = Some(p);
                            }
                            i += 3;
                        } else {
                            i += 1;
                        }
                    } else {
                        i += 1;
                    }
                }
                "--dir" => {
                    if i + 1 < args.len() {
                        data_dir = args[i + 1].clone();
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--dbfilename" => {
                    if i + 1 < args.len() {
                        db_filename = args[i + 1].clone();
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                "--shards" => {
                    if i + 1 < args.len() {
                        if let Ok(n) = args[i + 1].parse() {
                            if n > 0 {
                                num_shards = n;
                            }
                        }
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                _ => {
                    i += 1;
                }
            }
        }

        Config {
            port,
            role,
            master_replid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
            master_repl_offset: 0,
            master_host,
            master_port,
            data_dir,
            db_filename,
            num_shards,
        }
    }

    pub fn rdb_path(&self) -> PathBuf {
        Path::new(&self.data_dir).join(&self.db_filename)
    }
}
