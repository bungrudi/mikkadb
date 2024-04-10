use std::cmp::PartialEq;

enum RespState {
    Idle,
    ArrayDef { startpos: usize },
    BulkDef { startpos: usize },
    BulkData { startpos: usize },
    End,
    Error,
}

#[derive(Copy, Clone)]
pub struct Command<'a> {
    pub(crate) command: &'a str,
    pub(crate) data: &'a str,
}

struct Context<'a> {
    buffer: &'a[u8],
    read_len: usize,
    current_pos: usize,
    resp_state: RespState,
    data_length: usize,
    current_command: Command<'a>,
    commands: Vec<Command<'a>>,
    error_reason: String,
}

impl Context<'_> {
    const NO_ERROR: &'static str = "NO_ERROR";
    const PARSE_ERROR: &'static str = "PARSE_ERROR";
    const STATE_ERROR: &'static str = "STATE_ERROR";

    const EMPTY_STR: &'static str = "";

    fn char_asterisk(&mut self) {
        // parse the array length, move pointer to the end of the array length, change state to Idle
        match self.resp_state {
            RespState::Idle => {
               // self.resp_state = RespState::ArrayDef { startpos: self.current_pos + 1 }
                // TODO currently we do nothing with array and array length
                // expect the next character to be numeric, if not, throw an error.
                // after that just fast forward to the end of the array, or the end of carriage return + endline.
                self.current_pos += 1;
                while self.buffer[self.current_pos].is_ascii_digit() {
                    self.current_pos += 1;
                }
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
                    self.resp_state = RespState::BulkData { startpos: self.current_pos };

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

                        if(self.current_command.command == Context::EMPTY_STR) {
                            self.current_command.command = data;
                        } else {
                            self.current_command.data = data;
                            self.commands.push(self.current_command);
                            self.current_command = Command { command: Context::EMPTY_STR, data: Context::EMPTY_STR };
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
        if self.current_pos == 0 && (self.buffer[self.current_pos] != b'*' && self.buffer[self.current_pos] != b'$') {
            // TODO length should be parameterized
            let mut startpos = self.current_pos;
            let mut first_space = false;
            while true {
                if self.buffer[self.current_pos] == b' ' && !first_space {
                    let data = std::str::from_utf8(&self.buffer[startpos..self.current_pos]).unwrap();
                    self.current_command.command = data;
                    first_space = true;
                    startpos = self.current_pos + 1;
                }
                else if self.current_pos >= self.read_len ||
                    self.buffer[self.current_pos] == b'\r' ||
                    self.buffer[self.current_pos] == b'\n' {
                    let data = std::str::from_utf8(&self.buffer[startpos..self.current_pos]).unwrap();
                    if self.current_command.command == Context::EMPTY_STR {
                        self.current_command.command = data;
                    } else {
                        self.current_command.data = data;
                    }
                    self.commands.push(self.current_command);
                    break;
                }
                self.current_pos += 1;
            }

        } else {
            self.error_reason = Context::PARSE_ERROR.to_string();
        }
    }
}


// parse RESP using a simple state machine algorithm
pub fn parse_resp(buffer: &[u8], len: usize) -> Vec<Command> {
    let mut context = Context {
        buffer,
        read_len: len,
        current_pos: 0,
        resp_state: RespState::Idle,
        data_length: 0,
        current_command: Command { command: Context::EMPTY_STR, data: Context::EMPTY_STR },
        commands: Vec::new(),
        error_reason: Context::NO_ERROR.to_string(),
    };

    while context.current_pos < context.read_len {
        if context.error_reason != Context::NO_ERROR || matches!(context.resp_state, RespState::End) {
            break;
        }
        // TODO check end buffer
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

    context.commands
}


// write test here
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resp_command_pair() {
        let buffer = b"*2\r\n$4\r\nECHO\r\n$4\r\nHOLA\r\n";
        let commands = parse_resp(buffer,24);
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].command, "ECHO");
        assert_eq!(commands[0].data, "HOLA");
    }

    #[test]
    fn test_non_resp_single() {
        let buffer = b"PING\r\n";
        let commands = parse_resp(buffer,6);
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].command, "PING");
        assert_eq!(commands[0].data, "");
    }

    #[test]
    fn test_non_resp_pair() {
        let buffer = b"SET key00 val00\r\n";
        let commands = parse_resp(buffer,17);
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].command, "SET");
        assert_eq!(commands[0].data, "key00 val00");
    }
}