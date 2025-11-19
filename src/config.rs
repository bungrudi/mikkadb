use std::env;

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
}

impl Config {
    pub fn parse() -> Self {
        let args: Vec<String> = env::args().collect();
        let mut port = 6379;
        let mut role = ServerRole::Master;

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
        }
    }
}
