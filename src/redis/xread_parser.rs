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
    #[cfg(debug_assertions)]
    println!("\n[XReadParser::parse_xread] Starting with params: {:?}", params);

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

    #[cfg(debug_assertions)]
    println!("[XReadParser::parse_xread] Found STREAMS at position {}", streams_pos);

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

    #[cfg(debug_assertions)]
    println!("[XReadParser::parse_xread] Remaining params after STREAMS: {:?}", remaining);

    // Split remaining parameters into stream names and IDs
    let num_params = remaining.len();
    if num_params % 2 != 0 {
        return Err("-ERR wrong number of arguments for 'xread' command\r\n".to_string());
    }

    let mid = num_params / 2;
    let stream_names = remaining[..mid].to_vec();
    let stream_ids = remaining[mid..].to_vec();

    #[cfg(debug_assertions)]
    println!("[XReadParser::parse_xread] Split into:");
    #[cfg(debug_assertions)]
    println!("  - Stream names: {:?}", stream_names);
    #[cfg(debug_assertions)]
    println!("  - Stream IDs: {:?}", stream_ids);

    if stream_names.is_empty() || stream_ids.len() != stream_names.len() {
        return Err("-ERR wrong number of arguments for 'xread' command\r\n".to_string());
    }

    // Validate all IDs
    for id in &stream_ids {
        if !is_stream_id(id) {
            return Err(format!("-ERR Invalid stream ID format: {}\r\n", id));
        }
    }

    Ok(XReadParams {
        keys: stream_names,
        ids: stream_ids,
        block,
        count,
    })
}
