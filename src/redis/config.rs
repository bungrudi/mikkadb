#[derive(Clone)]
pub struct RedisConfig {
    pub port: String,
    pub addr: String,
    pub replicaof_host: Option<String>,
    pub replicaof_port: Option<String>,
    pub dir: String,
    pub dbfilename: String,
}

impl RedisConfig {
    pub fn new() -> Self {
        RedisConfig {
            port: "6379".to_string(),
            addr: "0.0.0.0".to_string(),
            replicaof_host: None,
            replicaof_port: None,
            dir: ".".to_string(),
            dbfilename: "dump.rdb".to_string(),
        }
    }
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self::new()
    }
}
