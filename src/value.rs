use std::collections::HashMap;


#[derive(Debug, PartialEq, Eq, Clone)]
pub enum RedisValue {
    String(String),
    List(Vec<String>),
    Hash(HashMap<String, String>),
    Stream(Vec<StreamEntry>),
}

#[derive(Clone,Debug, PartialEq, Eq)]
pub struct StreamEntry {
    pub id: String,
    pub fields: HashMap<String, String>,
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