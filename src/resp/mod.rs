mod parser;
mod command;
mod context;
mod state;

pub use parser::parse_resp;

#[macro_export]
macro_rules! process_command {
    ($self:ident) => {
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
                            $self.error_reason = "Too many commands".to_string();
                        }
                    },
                    None => { // we treat None as error. This should not happen.
                        $self.error_reason = Context::PARSE_ERROR.to_string();
                    }
                }
            },
            None => {
                // $self.error_reason = Context::STATE_ERROR.to_string();
            }
        }
    };
}
