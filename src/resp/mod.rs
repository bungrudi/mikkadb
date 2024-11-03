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
                let command_ = command.command;
                let params = command.data;

                let original_resp = std::str::from_utf8( &$self.buffer[command.buffer_start..command.buffer_end] ).unwrap();
                match RedisCommand::data(command_, params, original_resp) {
                    Some(redis_command) => {
                        if $self.command_index < 25 {
                            $self.commands[$self.command_index as usize] = redis_command;
                            $self.current_command = None;
                            $self.command_index += 1;
                        } else{
                            $self.error_reason = "Too many commands".into();
                        }
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
