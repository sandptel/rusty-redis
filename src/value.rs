use std::collections::HashMap;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum RedisValue {
    String(String),
    List(Vec<String>),
    Hash(HashMap<String, String>),
    Stream(Vec<StreamEntry>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
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
    pub fn validate_entry_id(new: StreamEntry, old: Option<&StreamEntry>) -> Option<bool> {
        if new.milliseconds_time <= 0 {
            if new.sequence_number > 0 {
                return Some(false);
            }
            return None;
        }

        match old {
            None => { Some(true) }
            Some(old_entry) => {
                if old_entry.milliseconds_time > new.milliseconds_time {
                    dbg!("old milli > new milli");
                    Some(false)
                } else if old_entry.milliseconds_time == new.milliseconds_time {
                    if old_entry.sequence_number >= new.sequence_number {
                        dbg!(
                            "old seq: {} > new seq: {} after milli equal",
                            old_entry.sequence_number,
                            new.sequence_number
                        );
                        return Some(false);
                    } else {
                        return Some(true);
                    }
                } else {
                    Some(true)
                }
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
        println!("{:?}",entry_id);
        if entry_id[1] == "*" {
            entry_id[1] = match old {
                Some(stream_entry) => {
                    let new_sequence_number = stream_entry.sequence_number + 1;
                    new_sequence_number.to_string()
                }
                None => { (0).to_string() }
            };
        }
        println!("{:?}",entry_id);
        let milliseconds_time = entry_id[0].parse().expect("Incorrect milliseconds time entered");
        let sequence_number = entry_id[1].parse().expect("Invalid Sequence Number Provided");
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
