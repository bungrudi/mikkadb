use std::string::ToString;
use crate::redis::RedisCommand;

macro_rules! process_command {
    ($self:ident) => {
        match &$self.current_command {
            Some(command) => {
                let command_ = command.command;
                let params = command.data;
                match RedisCommand::data(command_, params) {
                    Some(redis_command) => {
                        // print!("Command: {:?}", redis_command);

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

macro_rules! commands_len  {
    ($commands:expr) => {
        $commands.iter().filter(|&x| !x.is_none()).count()
    };
}

enum RespState {
    Idle,
    // ArrayDef,
    // BulkDef,
    BulkData,
    End,
    Error,
}

struct Command<'a> {
    command: &'a str,
    data: [&'a str; 5],
    num_params: u8
}

impl<'a> Command<'a> {
    const EMPTY_STR: &'static str = "";

    fn new(num_params:u8) -> Self {
        Command {
            command: Command::EMPTY_STR,
            data: [Command::EMPTY_STR; 5], // max 5 fow now
            num_params
        }
    }

    fn push_param(&mut self, param: &'a str) {
        for i in 0..self.data.len() {
            if self.data[i] == Command::EMPTY_STR {
                self.data[i] = param;
                break;
            }
        }
    }
}

struct Context<'a> {
    buffer: &'a[u8],
    read_len: usize,
    current_pos: usize,
    resp_state: RespState,
    data_length: usize,
    current_command: Option<Command<'a>>,
    command_index: u8,
    commands: [RedisCommand<'a>;25],
    error_reason: String,
}

impl Context<'_> {
    const NO_ERROR: &'static str = "NO_ERROR";
    const PARSE_ERROR: &'static str = "PARSE_ERROR";
    const STATE_ERROR: &'static str = "STATE_ERROR";

    // const EMPTY_STR: &'static str = "";

    fn char_asterisk(&mut self) {
        // parse the array length, move pointer to the end of the array length, change state to Idle
        match self.resp_state {
            RespState::Idle => {

                // expect the next character to be numeric, if not, throw an error.
                // after that just fast forward to the end of the array, or the end of carriage return + endline.
                self.current_pos += 1;
                let startpos = self.current_pos;
                while self.buffer[self.current_pos].is_ascii_digit() {
                    self.current_pos += 1;
                }
                // parse length of the array.
                // the length of the array is the number of params in current command.
                let num_params: u8 = std::str::from_utf8(&self.buffer[startpos..self.current_pos]).unwrap().parse().unwrap();
                // for every *n, create a new command object.
                // number of params is n-1 because the first param is the command itself.
                self.current_command = Some(Command::new(num_params-1));
                // check for end of buffer
                if self.current_pos == self.read_len {
                    self.resp_state = RespState::End;
                    return;
                }
                // when we first encounter a non-numeric character, we should expect a carriage return
                // otherwise assume it's the end of operation (because whatever character there might be leftover
                // from previous stream operation)
                if self.buffer[self.current_pos] == b'\r' {
                    if self.buffer[self.current_pos + 1] == b'\n' {
                        self.current_pos += 2;
                        self.resp_state = RespState::Idle;
                        return;
                    } else {
                        self.error_reason = Context::PARSE_ERROR.to_string();
                        return;
                    }
                } else {
                    self.resp_state = RespState::End;
                    return;
                }
            },
            _ => self.error_reason = Context::STATE_ERROR.to_string(),
        }
    }

    fn char_dollar(&mut self) {
        match self.resp_state {
            RespState::Idle => {
                // expect the next character to be numeric, if not, throw an error.
                // after that expect the next 2 characters to be carriage return + endline,
                // then parse the bulk data for the length of the data,
                // create command object, and move pointer at the end of the data
                self.current_pos += 1;
                let startpos = self.current_pos;
                while self.buffer[self.current_pos].is_ascii_digit() {
                    self.current_pos += 1;
                }
                // parse length of the data
                let length = std::str::from_utf8(&self.buffer[startpos..self.current_pos]).unwrap().parse().unwrap();
                self.data_length = length;

                // when we first encounter a non-numeric character, we should expect a carriage return
                if self.buffer[self.current_pos] == b'\r' && self.buffer[self.current_pos + 1] == b'\n' {
                    self.current_pos += 2;
                    self.resp_state = RespState::BulkData;
                } else {
                    self.resp_state = RespState::Error;
                    self.error_reason = format!("parse error at position {}", self.current_pos);
                    return;
                }

                // parse bulk data
                match self.resp_state {
                    RespState::BulkData { .. } => {
                        let mut endpos = self.current_pos + self.data_length;
                        if endpos >= self.read_len {
                            endpos = self.read_len - 1;
                        }
                        let data = std::str::from_utf8(&self.buffer[self.current_pos..endpos]).unwrap();
                        let command  = match self.current_command {
                            Some(ref mut command) => {
                                command
                            },
                            None => {
                                panic!("Current command not initialized when parsing bulk data");
                            }
                        };
                        if command.command == Command::EMPTY_STR {
                            command.command = data;
                        } else {
                            command.push_param(data);
                        }

                        // check if we have read all params.
                        // if we have, convert to RedisCommand and push to vector.

                        if command.data.iter().filter(|&x| x != &Command::EMPTY_STR).count() == command.num_params as usize {
                            process_command!(self);
                        }

                        // after this, expect the next to be carriage return + endline and then move the pointer.
                        // if not, it's end of operation.
                        if self.buffer[endpos] == b'\r' && self.buffer[endpos + 1] == b'\n' {
                            self.current_pos = endpos + 2;
                            self.resp_state = RespState::Idle;
                        } else {
                            self.resp_state = RespState::End;
                        }
                    },
                    _ => {
                        self.error_reason = Context::STATE_ERROR.to_string();
                        return;
                    }
                }
            },
            _ => self.error_reason = Context::STATE_ERROR.to_string(),
        }
    }

    fn char_carriage_return(&mut self) {
        match self.resp_state {
            RespState::Idle => {
                // expect the next character to be endline, if not, throw an error.
                // after that, move the pointer to the end of the endline, and change state to Idle
                if self.buffer[self.current_pos + 1] == b'\n' {
                    self.current_pos += 1;
                    self.resp_state = RespState::Idle;
                } else {
                    self.error_reason = Context::PARSE_ERROR.to_string();
                }
            },
            _ => self.error_reason = Context::STATE_ERROR.to_string(),
        }
    }

    fn handle_data_non_resp(&mut self) {
        // we should parse "SET key00 val00" as a single command,
        // resulting in command = "SET", data = "key00 val00".
        // we iterate the buffer and split the command and data by the first space character.
        // first we should validate that the string is not in RESP format.
        // we do this by validating that the first character is not an asterisk or dollar sign,
        // and pointer is at pos 0.

        // we don't now how many params we have, so we set it to 0.
        let mut command  = Command::new(0);

        if self.current_pos == 0 && (self.buffer[self.current_pos] != b'*' && self.buffer[self.current_pos] != b'$') {
            let mut startpos = self.current_pos;
            // let mut first_space = false;
            loop {
                if self.current_pos == self.read_len ||
                    self.buffer[self.current_pos] == b' ' ||
                    self.buffer[self.current_pos] == b'\r' ||
                    self.buffer[self.current_pos] == b'\n' {
                    let data = std::str::from_utf8(&self.buffer[startpos..self.current_pos]).unwrap();
                    // first_space = true;
                    if command.command == Command::EMPTY_STR {
                        command.command = data;
                    } else {
                        command.push_param(data);
                    }

                    if self.current_pos >= self.read_len {
                        break;
                    }
                    startpos = self.current_pos + 1;
                }

                self.current_pos += 1;
            }

            self.current_command = Some(command);

            process_command!(self);
        } else {
            self.error_reason = Context::PARSE_ERROR.to_string();
        }
    }

}


// parse RESP using a simple state machine algorithm
pub fn parse_resp(buffer: &[u8], len: usize) -> [RedisCommand;25] {
    const ARRAY_REPEAT_VALUE: RedisCommand<'_> = RedisCommand::None;
    let mut context = Context {
        buffer,
        read_len: len,
        current_pos: 0,
        resp_state: RespState::Idle,
        data_length: 0,
        current_command: None,
        command_index: 0,
        commands: [ARRAY_REPEAT_VALUE;25],
        error_reason: Context::NO_ERROR.to_string(),
    };

    while context.current_pos < context.read_len {
        if context.error_reason != Context::NO_ERROR || matches!(context.resp_state, RespState::End) {
            break;
        }
        // end if length > buffer length or end of string length
        if context.current_pos >= context.read_len {
            break;
        }

        // if let RespState::End = context.resp_state {
        //     break;
        // }

        match context.buffer[context.current_pos] {
            b'*' => context.char_asterisk(),
            b'$' => context.char_dollar(),
            b'\r' => context.char_carriage_return(),
            _ => {
                // panic!("Invalid RESP character at position {}", context.current_pos)
                context.handle_data_non_resp();
                break;
            },
        }
        // context.current_pos += 1;
    }

    if context.error_reason != Context::NO_ERROR {
        panic!("Error parsing RESP: {}", context.error_reason);
    }

    // if there is leftover command, push it to vector
    // match &context.current_command {
    //     Some(ref command) => {
    //         process_command!(context);
    //     },
    //     None => (),
    // }
    // just call macro directly, the check like the code above is redundant.
    process_command!(context);

    context.commands
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resp_command_pair() {
        let buffer = b"*2\r\n$4\r\nECHO\r\n$4\r\nHOLA\r\n";
        let commands = parse_resp(buffer,24);
        assert_eq!(commands_len!(commands), 1);
        match commands[0] {
            RedisCommand::Echo { data } => assert_eq!(data, "HOLA"),
            _ => panic!("Invalid command"),
        }
    }

    #[test]
    fn test_resp_command_single() {
        let buffer = b"*1\r\n$4\r\nping\r\n";
        let commands = parse_resp(buffer, buffer.len());
        assert_eq!(commands_len!(commands), 1);
        match commands[0] {
            RedisCommand::Ping => (),
            _ => panic!("Invalid command"),
        }
    }

    #[test]
    fn test_resp_set_command() {
        let buffer = b"*3\r\n$3\r\nSET\r\n$5\r\nkey01\r\n$5\r\nval01\r\n";
        let commands = parse_resp(buffer, buffer.len());
        assert_eq!(commands_len!(commands), 1);
        match commands[0] {
            RedisCommand::Set { key, value, ttl} => {
                assert_eq!(key, "key01");
                assert_eq!(value, "val01");
                assert_eq!(ttl, None);
            },
            _ => panic!("Invalid command"),
        }
    }

    #[test]
    fn test_resp_set_ttl_and_echo() {
        let buffer = b"*5\r\n$3\r\nSET\r\n$5\r\nkey01\r\n$5\r\nval01\r\n$2\r\nex\r\n$2\r\n60\r\n*2\r\n$4\r\nECHO\r\n$5\r\nHELLO\r\n";
        let commands = parse_resp(buffer, buffer.len());
        assert_eq!(commands_len!(commands), 2);
        match commands[0] {
            RedisCommand::Set { key, value, ttl } => {
                assert_eq!(key, "key01");
                assert_eq!(value, "val01");
                assert_eq!(ttl, Some(60000));
            },
            _ => panic!("Invalid command"),
        }
        match commands[1] {
            RedisCommand::Echo { data } => assert_eq!(data, "HELLO"),
            _ => panic!("Invalid command"),
        }
    }

    #[test]
    fn test_resp_five_commands_various_types_with_invalid() {
        let buffer = b"*3\r\n$3\r\nSET\r\n$5\r\nkey01\r\n$5\r\nval01\r\n*2\r\n$3\r\nGET\r\n$5\r\nkey01\r\n*2\r\n$4\r\nECHO\r\n$5\r\nHELLO\r\n*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nFAKE\r\n$5\r\nPARAM\r\n";
        let commands = parse_resp(buffer, buffer.len());
        assert_eq!(commands_len!(commands), 5);
        match commands[0] {
            RedisCommand::Set { key, value, ttl } => {
                assert_eq!(key, "key01");
                assert_eq!(value, "val01");
                assert_eq!(ttl, None);
            },
            _ => panic!("Invalid command"),
        }
        match commands[1] {
            RedisCommand::Get { key } => {
                assert_eq!(key, "key01");
            },
            _ => panic!("Invalid command"),
        }
        match commands[2] {
            RedisCommand::Echo { data } => {
                assert_eq!(data, "HELLO");
            },
            _ => panic!("Invalid command"),
        }
        match commands[3] {
            RedisCommand::Ping => (),
            _ => panic!("Invalid command"),
        }
        match &commands[4] {
            RedisCommand::Error { message } => {
                assert_eq!(message, "Unknown command: FAKE");
            },
            _ => panic!("Invalid command"),
        }
    }
    #[test]
    fn test_resp_six_commands_various_types_all_valid() {
        let buffer = b"*3\r\n$3\r\nSET\r\n$5\r\nkey01\r\n$5\r\nval01\r\n*2\r\n$3\r\nGET\r\n$5\r\nkey01\r\n*2\r\n$4\r\nECHO\r\n$5\r\nHELLO\r\n*1\r\n$4\r\nPING\r\n*5\r\n$3\r\nSET\r\n$5\r\nkey02\r\n$5\r\nval02\r\n$2\r\nEX\r\n$2\r\n60\r\n*2\r\n$3\r\nGET\r\n$5\r\nkey02\r\n";
        let commands = parse_resp(buffer, buffer.len());
        assert_eq!(commands_len!(commands), 6);
        match commands[0] {
            RedisCommand::Set { key, value, ttl } => {
                assert_eq!(key, "key01");
                assert_eq!(value, "val01");
                assert_eq!(ttl, None);
            },
            _ => panic!("Invalid command"),
        }
        match commands[1] {
            RedisCommand::Get { key } => {
                assert_eq!(key, "key01");
            },
            _ => panic!("Invalid command"),
        }
        match commands[2] {
            RedisCommand::Echo { data } => {
                assert_eq!(data, "HELLO");
            },
            _ => panic!("Invalid command"),
        }
        match commands[3] {
            RedisCommand::Ping => (),
            _ => panic!("Invalid command"),
        }
        match commands[4] {
            RedisCommand::Set { key, value, ttl } => {
                assert_eq!(key, "key02");
                assert_eq!(value, "val02");
                assert_eq!(ttl, Some(60000));
            },
            _ => panic!("Invalid command"),
        }
        match commands[5] {
            RedisCommand::Get { key } => {
                assert_eq!(key, "key02");
            },
            _ => panic!("Invalid command"),
        }
    }

    #[test]
    fn test_non_resp_single() {
        let buffer = b"PING\r\n";
        let commands = parse_resp(buffer,6);
        assert_eq!(commands_len!(commands), 1);
        match commands[0] {
            RedisCommand::Ping => (),
            _ => panic!("Invalid command"),
        }
    }

    #[test]
    fn test_non_resp_single_no_ending_whitespace() {
        let buffer = b"PING";
        let commands = parse_resp(buffer,4);
        assert_eq!(commands_len!(commands), 1);
        match commands[0] {
            RedisCommand::Ping => (),
            _ => panic!("Invalid command"),
        }
    }

    #[test]
    fn test_non_resp_pair() {
        let buffer = b"SET key00 val00\r\n";
        let commands = parse_resp(buffer,17);
        assert_eq!(commands_len!(commands), 1);
        match commands[0] {
            RedisCommand::Set { key, value, ttl } => {
                assert_eq!(key, "key00");
                assert_eq!(value, "val00");
                assert_eq!(ttl, None);
            },
            _ => panic!("Invalid command"),
        }
    }

    #[test]
    fn test_resp_set_px() {
        let buffer = b"*5\r\n$3\r\nSET\r\n$5\r\nkey01\r\n$5\r\nval01\r\n$2\r\nPX\r\n$4\r\n1500\r\n";
        let commands = parse_resp(buffer, buffer.len());
        assert_eq!(commands_len!(commands), 1);
        match commands[0] {
            RedisCommand::Set { key, value, ttl } => {
                assert_eq!(key, "key01");
                assert_eq!(value, "val01");
                assert_eq!(ttl, Some(1500));
            },
            _ => panic!("Invalid command"),
        }
    }

    #[test]
    fn test_resp_set_ex() {
        let buffer = b"*5\r\n$3\r\nSET\r\n$5\r\nkey02\r\n$5\r\nval02\r\n$2\r\nEX\r\n$2\r\n60\r\n";
        let commands = parse_resp(buffer, buffer.len());
        assert_eq!(commands_len!(commands), 1);
        match commands[0] {
            RedisCommand::Set { key, value, ttl } => {
                assert_eq!(key, "key02");
                assert_eq!(value, "val02");
                assert_eq!(ttl, Some(60000));
            },
            _ => panic!("Invalid command"),
        }
    }
}

