use super::context::Context;
use super::state::RespState;
use crate::redis::RedisCommand;
use crate::process_command;

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

    macro_rules! commands_len  {
        ($commands:expr) => {
            $commands.iter().filter(|&x| !is_none(&x)).count()
        };
    }

    fn is_none(command: &RedisCommand) -> bool {
        matches!(command, RedisCommand::None)
    }

    // All test functions remain the same
    // ... (include all the test functions here)
}
