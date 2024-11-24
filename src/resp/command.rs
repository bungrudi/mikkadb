pub struct Command {
    pub buffer_start: usize,
    pub buffer_end: usize,
    pub command: String,
    pub data: Vec<String>,
    pub num_params: u8
}

impl Command {
    pub const EMPTY_STR: &'static str = "";

    pub fn new(num_params:u8, buffer_start: usize) -> Self {
        Command {
            command: String::new(),
            data: Vec::with_capacity(num_params as usize),
            buffer_start,
            buffer_end: 0,
            num_params
        }
    }

    pub fn push_param(&mut self, param: String) {
        self.data.push(param);
    }
}
