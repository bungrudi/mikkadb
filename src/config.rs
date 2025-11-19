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

        let mut i = 0;
        while i < args.len() {
            match args[i].as_str() {
                "--port" => {
                    if i + 1 < args.len() {
                        if let Ok(p) = args[i + 1].parse() {
                            port = p;
                        }
                    }
                }
                "--replicaof" => {
                    if i + 1 < args.len() {
                        let parts: Vec<&str> = args[i + 1].split_whitespace().collect();
                        if parts.len() >= 2 {
                            master_host = Some(parts[0].to_string());
                            if let Ok(p) = parts[1].parse() {
                                master_port = Some(p);
                            }
                            role = ServerRole::Slave;
                        }
                        // Handle case where arguments might be separate tokens if not quoted (though usually passed as one string in tests, but shell splitting might vary)
                        // Actually, standard args parsing splits by space unless quoted.
                        // If run as: --replicaof "localhost 6379", it's one arg.
                        // If run as: --replicaof localhost 6379, it's two args.
                        // The tester usually passes it as two separate arguments to the binary if not using a shell script wrapper that quotes it.
                        // But our `your_program.sh` passes "$@".
                        // Let's assume the tester passes "--replicaof" "localhost" "6379" OR "--replicaof" "localhost 6379".
                        // The previous implementation just set role = Slave.
                        // Let's be robust.
                    }
                    // Re-implementing robust parsing below
                }
                "--dir" => {
                    if i + 1 < args.len() {
                        data_dir = args[i + 1].clone();
                    }
                }
                "--dbfilename" => {
                    if i + 1 < args.len() {
                        db_filename = args[i + 1].clone();
                    }
                }
                _ => {}
            }
            i += 1;
        }
        
        // Second pass or cleaner pass
        let mut i = 0;
        while i < args.len() {
             match args[i].as_str() {
                "--port" => {
                    if i + 1 < args.len() {
                        if let Ok(p) = args[i + 1].parse() {
                            port = p;
                        }
                    }
                }
                "--replicaof" => {
                    role = ServerRole::Slave;
                    if i + 2 < args.len() {
                         // Assume format: --replicaof <host> <port>
                         master_host = Some(args[i+1].clone());
                         if let Ok(p) = args[i+2].parse() {
                             master_port = Some(p);
                         }
                    }
                }
                "--dir" => {
                    if i + 1 < args.len() {
                        data_dir = args[i + 1].clone();
                    }
                }
                "--dbfilename" => {
                    if i + 1 < args.len() {
                        db_filename = args[i + 1].clone();
                    }
                }
                _ => {}
            }
            i += 1;
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
        }
    }

    pub fn rdb_path(&self) -> PathBuf {
        Path::new(&self.data_dir).join(&self.db_filename)
    }
}
