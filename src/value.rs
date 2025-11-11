use std::collections::HashMap;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum RedisValue {
    String(String),
    List(Vec<String>),
    Hash(HashMap<String, String>),
    Stream(Vec<StreamEntry>),
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct StreamEntry {
    pub id: String,
    pub milliseconds_time: usize,
    pub sequence_number: usize,
    pub fields: HashMap<String, String>,
}

impl StreamEntry {
    // here I return Option<bool> : None denotes "(error) ERR The ID specified in XADD must be greater than 0-0"
    // Option<true> denotes return entry_id
    // Option<false> denotes (error) ERR The ID specified in XADD is equal or smaller than the target stream top item
    pub fn validate_entry_id(new: &mut StreamEntry, old: Option<&StreamEntry>) -> Option<bool> {
        // First check if we have an old entry to compare against
        match old {
            Some(old_entry) => {
                // Compare with existing entry
                if old_entry.milliseconds_time > new.milliseconds_time {
                    return Some(false);
                } else if old_entry.milliseconds_time == new.milliseconds_time {
                    if old_entry.sequence_number >= new.sequence_number {
                        return Some(false);
                    } else {
                        return Some(true);
                    }
                } else {
                    // old_entry.milliseconds_time < new.milliseconds_time
                    return Some(true);
                }
            }
            None => {
                // No old entry, just validate against 0-0
                if new.milliseconds_time == 0 && new.sequence_number == 0 {
                    return None;
                }
                return Some(true);
            }
        }
    }
    // this is very fragile as giving wrong entry_ids will break this unwrap() inside a the map(||)
    pub fn from(
        id: String,
        fields: HashMap<String, String>,
        old: Option<&StreamEntry>
    ) -> StreamEntry {
        let mut entry_id: Vec<String> = id
            .split_terminator('-')
            .map(|x| x.trim().to_string())
            .collect();

        let milliseconds_time: usize = entry_id[0]
            .parse()
            .expect("Incorrect milliseconds time entered");

        // Handle auto-generated sequence number
        if entry_id[1] == "*" {
            entry_id[1] = match old {
                Some(stream_entry) => {
                    // If the milliseconds time matches the last entry, increment sequence
                    if stream_entry.milliseconds_time == milliseconds_time {
                        (stream_entry.sequence_number + 1).to_string()
                    } else {
                        // Different milliseconds time, start from 0
                        // Unless milliseconds_time is 0, then start from 1
                        if milliseconds_time == 0 {
                            "1".to_string()
                        } else {
                            "0".to_string()
                        }
                    }
                }
                None => {
                    // No previous entries
                    if milliseconds_time == 0 {
                        "1".to_string()
                    } else {
                        "0".to_string()
                    }
                }
            };
        }

        let sequence_number = entry_id[1].parse().expect("Invalid Sequence Number Provided");
        // println!("milli: {}, sequence: {}", milliseconds_time, sequence_number);
        StreamEntry { id, milliseconds_time, sequence_number, fields }
    }
}

impl RedisValue {
    pub fn from_string(value: String) -> RedisValue {
        RedisValue::String(value)
    }
    pub fn from_list(list: Vec<String>) -> RedisValue {
        RedisValue::List(list)
    }
    pub fn from_stream(entries: Vec<StreamEntry>) -> Self {
        RedisValue::Stream(entries)
    }

    pub fn get_response(&self) -> String {
        match self {
            // Simple string value - return as bulk string
            RedisValue::String(s) => { format!("${}\r\n{}\r\n", s.len(), s) }

            // List - return as array of bulk strings
            RedisValue::List(list) => {
                if list.is_empty() {
                    "*0\r\n".to_string()
                } else {
                    let mut response = format!("*{}\r\n", list.len());
                    for item in list {
                        response.push_str(&format!("${}\r\n{}\r\n", item.len(), item));
                    }
                    response
                }
            }

            // Hash - return as array of field-value pairs (flattened)
            RedisValue::Hash(hash) => {
                if hash.is_empty() {
                    "*0\r\n".to_string()
                } else {
                    // Each key-value pair becomes 2 elements in the array
                    let mut response = format!("*{}\r\n", hash.len() * 2);
                    for (key, value) in hash {
                        response.push_str(&format!("${}\r\n{}\r\n", key.len(), key));
                        response.push_str(&format!("${}\r\n{}\r\n", value.len(), value));
                    }
                    response
                }
            }

            RedisValue::Stream(_) => "+stream\r\n".to_string(),
        }
    }

    // The TYPE command returns the type of value stored at a given key. These types include: string, list, set, zset, hash, stream, and vectorset.
    // Server should respond with +string\r\n, which is string encoded as a simple string.
    pub fn get_type_response(&self) -> String {
        match self {
            // Simple string value - return as bulk string
            RedisValue::String(s) => { "+string\r\n".to_string() }

            // List - return as array of bulk strings
            RedisValue::List(list) => { "+list\r\n".to_string() }

            RedisValue::Hash(list) => { "+stream\r\n".to_string() }

            RedisValue::Stream(_) => "+stream\r\n".to_string(),
        }
    }

    /// Returns null response when key doesn't exist
    pub fn get_null_response() -> String {
        "$-1\r\n".to_string()
    }
}
