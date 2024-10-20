pub struct Command<'a> {
    pub buffer_start: usize,
    pub buffer_end: usize,
    pub command: &'a str,
    pub data: [&'a str; 5],
    pub num_params: u8
}

impl<'a> Command<'a> {
    pub const EMPTY_STR: &'static str = "";

    pub fn new(num_params:u8, buffer_start: usize) -> Self {
        Command {
            command: Command::EMPTY_STR,
            data: [Command::EMPTY_STR; 5], // max 5 fow now
            buffer_start,
            buffer_end: 0,
            num_params
        }
    }

    pub fn push_param(&mut self, param: &'a str) {
        for i in 0..self.data.len() {
            if self.data[i] == Command::EMPTY_STR {
                self.data[i] = param;
                break;
            }
        }
    }
}
