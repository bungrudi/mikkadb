use std::borrow::Cow;
use super::context::Context;
use super::state::RespState;
use crate::redis::RedisCommand;
use crate::process_command;

// parse RESP using a simple state machine algorithm with zero-copy optimizations
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
        error_reason: Cow::Borrowed(Context::NO_ERROR),
    };

    while context.current_pos < context.read_len {
        if context.error_reason != Context::NO_ERROR || matches!(context.resp_state, RespState::End) {
            break;
        }
        // end if length > buffer length or end of string length
        if context.current_pos >= context.read_len {
            break;
        }

        match context.buffer[context.current_pos] {
            b'*' => context.char_asterisk(),
            b'$' => context.char_dollar(),
            b'\r' => context.char_carriage_return(),
            _ => {
                context.handle_data_non_resp();
                break;
            },
        }
    }

    if context.error_reason != Context::NO_ERROR {
        panic!("Error parsing RESP: {}", context.error_reason);
    }

    process_command!(context);

    context.commands
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! commands_len  {
        ($commands:expr) => {
            $commands.iter().filter(|&x| !is_none(&x)).count()
        };
    }

    fn is_none(command: &RedisCommand) -> bool {
        matches!(command, RedisCommand::None)
    }

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
        match &commands[0] {
            RedisCommand::Set { key, value, ttl, original_resp} => {
                assert_eq!(*key, "key01");
                assert_eq!(*value, "val01");
                assert_eq!(*ttl, None);
                assert_eq!(original_resp, std::str::from_utf8(buffer).unwrap());
            },
            _ => panic!("Invalid command"),
        }
    }

    #[test]
    fn test_resp_set_ttl_and_echo() {
        let buffer = b"*5\r\n$3\r\nSET\r\n$5\r\nkey01\r\n$5\r\nval01\r\n$2\r\nex\r\n$2\r\n60\r\n*2\r\n$4\r\nECHO\r\n$5\r\nHELLO\r\n";
        let commands = parse_resp(buffer, buffer.len());
        assert_eq!(commands_len!(commands), 2);
        match &commands[0] {
            RedisCommand::Set { key, value, ttl, original_resp } => {
                assert_eq!(*key, "key01");
                assert_eq!(*value, "val01");
                assert_eq!(*ttl, Some(60000));
                assert_eq!(original_resp, "*5\r\n$3\r\nSET\r\n$5\r\nkey01\r\n$5\r\nval01\r\n$2\r\nex\r\n$2\r\n60\r\n");
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
        match &commands[0] {
            RedisCommand::Set { key, value, ttl, original_resp } => {
                assert_eq!(*key, "key01");
                assert_eq!(*value, "val01");
                assert_eq!(*ttl, None);
                assert_eq!(*original_resp, "*3\r\n$3\r\nSET\r\n$5\r\nkey01\r\n$5\r\nval01\r\n");
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
        match &commands[0] {
            RedisCommand::Set { key, value, ttl, original_resp } => {
                assert_eq!(*key, "key01");
                assert_eq!(*value, "val01");
                assert_eq!(*ttl, None);
                assert_eq!(original_resp, "*3\r\n$3\r\nSET\r\n$5\r\nkey01\r\n$5\r\nval01\r\n");
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
            RedisCommand::Set { key, value, ttl, original_resp } => {
                assert_eq!(*key, "key02");
                assert_eq!(*value, "val02");
                assert_eq!(*ttl, Some(60000));
                assert_eq!(*original_resp, "*5\r\n$3\r\nSET\r\n$5\r\nkey02\r\n$5\r\nval02\r\n$2\r\nEX\r\n$2\r\n60\r\n");
            },
            _ => panic!("Invalid command"),
        }
        match &commands[5] {
            RedisCommand::Get { key } => {
                assert_eq!(*key, "key02");
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
        match &commands[0] {
            RedisCommand::Set { key, value, ttl, original_resp } => {
                assert_eq!(*key, "key00");
                assert_eq!(*value, "val00");
                assert_eq!(*ttl, None);
                assert_eq!(original_resp, std::str::from_utf8(buffer).unwrap());
            },
            _ => panic!("Invalid command"),
        }
    }

    #[test]
    fn test_resp_set_px() {
        let buffer = b"*5\r\n$3\r\nSET\r\n$5\r\nkey01\r\n$5\r\nval01\r\n$2\r\nPX\r\n$4\r\n1500\r\n";
        let commands = parse_resp(buffer, buffer.len());
        assert_eq!(commands_len!(commands), 1);
        match &commands[0] {
            RedisCommand::Set { key, value, ttl, original_resp } => {
                assert_eq!(*key, "key01");
                assert_eq!(*value, "val01");
                assert_eq!(*ttl, Some(1500));
                assert_eq!(original_resp, std::str::from_utf8(buffer).unwrap());
            },
            _ => panic!("Invalid command"),
        }
    }

    #[test]
    fn test_resp_set_ex() {
        let buffer = b"*5\r\n$3\r\nSET\r\n$5\r\nkey02\r\n$5\r\nval02\r\n$2\r\nEX\r\n$2\r\n60\r\n";
        let commands = parse_resp(buffer, buffer.len());
        assert_eq!(commands_len!(commands), 1);
        match &commands[0] {
            RedisCommand::Set { key, value, ttl, original_resp } => {
                assert_eq!(*key, "key02");
                assert_eq!(*value, "val02");
                assert_eq!(*ttl, Some(60000));
                assert_eq!(original_resp, std::str::from_utf8(buffer).unwrap());
            },
            _ => panic!("Invalid command"),
        }
    }
}
