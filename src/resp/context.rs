use std::borrow::Cow;
use super::state::RespState;
use super::command::Command;
use crate::redis::RedisCommand;
use crate::process_command;

pub struct Context {
    pub buffer: Vec<u8>,
    pub read_len: usize,
    pub current_pos: usize,
    pub resp_state: RespState,
    pub data_length: usize,
    pub current_command: Option<Command>,
    pub command_index: u8,
    pub commands: Vec<RedisCommand>,
    pub error_reason: Cow<'static, str>,
}

impl Context {
    pub const NO_ERROR: &'static str = "NO_ERROR";
    pub const PARSE_ERROR: &'static str = "PARSE_ERROR";
    pub const STATE_ERROR: &'static str = "STATE_ERROR";

    // const EMPTY_STR: &'static str = "";

    pub fn char_asterisk(&mut self) {
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
                let mut current_command = Command::new(num_params-1, startpos-1);
                // check for end of buffer
                if self.current_pos == self.read_len {
                    self.resp_state = RespState::End;
                    current_command.buffer_end = self.current_pos;
                    self.current_command = Some(current_command);
                    return;
                }
                // when we first encounter a non-numeric character, we should expect a carriage return
                // otherwise assume it's the end of operation (because whatever character there might be leftover
                // from previous stream operation)
                if self.buffer[self.current_pos] == b'\r' {
                    if self.buffer[self.current_pos + 1] == b'\n' {
                        self.current_pos += 2;
                        self.resp_state = RespState::Idle;
                        current_command.buffer_end = self.current_pos;
                        self.current_command = Some(current_command);
                        return;
                    } else {
                        self.error_reason = Cow::Borrowed(Context::PARSE_ERROR);
                        return;
                    }
                } else {
                    self.resp_state = RespState::End;
                    current_command.buffer_end = self.current_pos;
                    self.current_command = Some(current_command);
                    return;
                }
            },
            _ => self.error_reason = Cow::Borrowed(Context::STATE_ERROR),
        }
    }

    pub fn char_dollar(&mut self) {
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
                    self.error_reason = Cow::Owned(format!("parse error at position {}", self.current_pos));
                    return;
                }

                // parse bulk data
                match self.resp_state {
                    RespState::BulkData { .. } => {
                        let mut endpos = self.current_pos + self.data_length;
                        if endpos >= self.read_len {
                            endpos = self.read_len - 1;
                        }
                        let data = std::str::from_utf8(&self.buffer[self.current_pos..endpos]).unwrap().to_string();

                        { // enclosing the block to limit the scope of mutable borrow
                            let command = match self.current_command {
                                Some(ref mut command) => {
                                    command
                                },
                                None => {
                                    panic!("Current command not initialized when parsing bulk data");
                                }
                            };
                            if command.command.is_empty() {
                                command.command = data;
                            } else {
                                command.push_param(data);
                            }
                        }

                        // after this, expect the next to be carriage return + endline and then move the pointer.
                        // if not, it's end of operation.
                        if self.buffer[endpos] == b'\r' && self.buffer[endpos + 1] == b'\n' {
                            self.current_pos = endpos + 2;
                            self.resp_state = RespState::Idle;
                        } else {
                            self.resp_state = RespState::End;
                        }
                        match self.current_command {
                            Some(ref mut command) => {
                                command.buffer_end = self.current_pos;
                            },
                            None => {
                            }
                        };

                        // check if we have read all params.
                        // if we have, convert to RedisCommand and push to vector.

                        if let Some(command) = &self.current_command {
                            if command.data.iter().filter(|&x| x != &Command::EMPTY_STR).count() == command.num_params as usize {
                                // TODO encapsulate into inline function or macro
                                match &self.current_command {
                                    Some(command) => {
                                        let command_ = command.command.to_string();
                                        let params: Vec<String> = command.data.iter().map(|s| s.to_string()).collect();
                                        let original_resp = std::str::from_utf8(&self.buffer[command.buffer_start..command.buffer_end]).unwrap().to_string();
                                        match RedisCommand::data(command_, &params, original_resp) {
                                            Some(redis_command) => {
                                                self.commands.push(redis_command);
                                                self.current_command = None;
                                                self.command_index += 1;
                                            },
                                            None => {
                                                // None we just continue...
                                                // it means the command is not complete, maybe missing parameter.
                                            }
                                        }
                                    },
                                    None => {
                                        // $self.error_reason = Context::STATE_ERROR.to_string();
                                    }
                                }
                            }
                        }
                    },
                    _ => {
                        self.error_reason = Cow::Borrowed(Context::STATE_ERROR);
                        return;
                    }
                }
            },
            _ => self.error_reason = Cow::Borrowed(Context::STATE_ERROR),
        }
    }

    pub fn char_carriage_return(&mut self) {
        match self.resp_state {
            RespState::Idle => {
                // expect the next character to be endline, if not, throw an error.
                // after that, move the pointer to the end of the endline, and change state to Idle
                if self.buffer[self.current_pos + 1] == b'\n' {
                    self.current_pos += 1;
                    self.resp_state = RespState::Idle;
                } else {
                    self.error_reason = Cow::Borrowed(Context::PARSE_ERROR);
                }
            },
            _ => self.error_reason = Cow::Borrowed(Context::STATE_ERROR),
        }
    }

    pub fn handle_data_non_resp(&mut self) {
        // we should parse "SET key00 val00" as a single command,
        // resulting in command = "SET", data = "key00 val00".
        // we iterate the buffer and split the command and data by the first space character.
        // first we should validate that the string is not in RESP format.
        // we do this by validating that the first character is not an asterisk or dollar sign,
        // and pointer is at pos 0.

        // we don't now how many params we have, so we set it to 0.
        let mut command  = Command::new(0, 0);

        if self.current_pos == 0 && (self.buffer[self.current_pos] != b'*' && self.buffer[self.current_pos] != b'$') {
            let mut startpos = self.current_pos;
            // let mut first_space = false;
            loop {
                if self.current_pos == self.read_len ||
                    self.buffer[self.current_pos] == b' ' ||
                    self.buffer[self.current_pos] == b'\r' ||
                    self.buffer[self.current_pos] == b'\n' {
                    let data = std::str::from_utf8(&self.buffer[startpos..self.current_pos]).unwrap().to_string();
                    // first_space = true;
                    if command.command.is_empty() {
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

            command.buffer_end = self.current_pos;
            self.current_command = Some(command);

            process_command!(self);
        } else {
            self.error_reason = Cow::Borrowed(Context::PARSE_ERROR);
        }
    }
}
