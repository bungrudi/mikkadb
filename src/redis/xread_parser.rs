#[derive(Debug)]
pub struct XReadParams {
    pub keys: Vec<String>,
    pub ids: Vec<String>,
    pub block: Option<u64>,
    pub count: Option<usize>,
}

fn is_stream_id(s: &str) -> bool {
    s == "0" || s == "$" || s.contains('-') || s.parse::<u64>().is_ok()
}

pub fn parse_xread(params: &[String]) -> Result<XReadParams, String> {
    let mut block: Option<u64> = None;
    let mut count: Option<usize> = None;
    let mut streams_index = None;

    // First find the STREAMS keyword
    for (i, param) in params.iter().enumerate() {
        if param.eq_ignore_ascii_case("STREAMS") {
            streams_index = Some(i);
            break;
        }
    }

    let streams_pos = match streams_index {
        Some(pos) => pos,
        None => return Err("-ERR Missing 'STREAMS' keyword\r\n".to_string()),
    };

    // Process parameters before STREAMS
    let mut i = 0;
    while i < streams_pos {
        match params[i].to_ascii_uppercase().as_str() {
            "BLOCK" => {
                if i + 1 < streams_pos {
                    if let Ok(block_value) = params[i + 1].parse::<u64>() {
                        block = Some(block_value);
                        i += 2;
                    } else {
                        return Err("-ERR syntax error in BLOCK parameter\r\n".to_string());
                    }
                } else {
                    return Err("-ERR syntax error in BLOCK parameter\r\n".to_string());
                }
            },
            "COUNT" => {
                if i + 1 < streams_pos {
                    if let Ok(count_value) = params[i + 1].parse::<usize>() {
                        count = Some(count_value);
                        i += 2;
                    } else {
                        return Err("-ERR syntax error in COUNT parameter\r\n".to_string());
                    }
                } else {
                    return Err("-ERR syntax error in COUNT parameter\r\n".to_string());
                }
            },
            _ => {
                return Err("-ERR syntax error in XREAD command\r\n".to_string());
            }
        }
    }

    // Process parameters after STREAMS
    let remaining = &params[streams_pos + 1..];
    if remaining.is_empty() {
        return Err("-ERR wrong number of arguments for 'xread' command\r\n".to_string());
    }

    // Find the first ID in the remaining parameters
    let mut id_pos = None;
    for (i, param) in remaining.iter().enumerate() {
        if is_stream_id(param) {
            id_pos = Some(i);
            break;
        }
    }

    // If no ID is found, use the second parameter as ID
    let id_pos = match id_pos {
        Some(pos) => pos,
        None => {
            if remaining.len() < 2 {
                return Err("-ERR wrong number of arguments for 'xread' command\r\n".to_string());
            }
            1
        }
    };

    // Everything before the ID position is a stream name
    let stream_names = remaining[..id_pos].to_vec();
    if stream_names.is_empty() {
        return Err("-ERR wrong number of arguments for 'xread' command\r\n".to_string());
    }

    // The ID is used for all streams
    let id = remaining[id_pos].clone();
    let ids = vec![id; stream_names.len()];

    Ok(XReadParams {
        keys: stream_names,
        ids,
        block,
        count,
    })
}
