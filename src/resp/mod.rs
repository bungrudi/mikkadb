pub mod context;
pub mod parser;
pub mod command;
pub mod state;

pub use self::parser::parse_resp;

#[macro_export]
macro_rules! process_command {
    ($self:expr) => {
        match &$self.current_command {
            Some(command) => {
                let command_ = command.command.clone();
                let params = &command.data;
                let original_resp = std::str::from_utf8(&$self.buffer[command.buffer_start..command.buffer_end])
                    .unwrap()
                    .to_string();
                match RedisCommand::data(command_, &params, original_resp) {
                    Some(redis_command) => {
                        $self.commands.push(redis_command);
                        $self.current_command = None;
                        $self.command_index += 1;
                    },
                    None => {
                        $self.error_reason = Context::PARSE_ERROR.into();
                    }
                }
            },
            None => (),
        }
    };
}
