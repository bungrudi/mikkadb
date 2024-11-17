pub struct Command {
    pub buffer_start: usize,
    pub buffer_end: usize,
    pub command: String,
    pub data: [String; 5],
    pub num_params: u8
}

impl Command {
    pub const EMPTY_STR: &'static str = "";

    pub fn new(num_params:u8, buffer_start: usize) -> Self {
        Command {
            command: String::new(),
            data: [String::new(), String::new(), String::new(), String::new(), String::new()],
            buffer_start,
            buffer_end: 0,
            num_params
        }
    }

    pub fn push_param(&mut self, param: String) {
        for i in 0..self.data.len() {
            if self.data[i].is_empty() {
                self.data[i] = param;
                break;
            }
        }
    }
}
