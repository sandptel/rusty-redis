#![allow(unused_imports)]
use std::error::Error;
use std::vec;
use tokio::net::TcpListener;
use tokio::time::{ sleep_until, Instant, Duration };
use std::future::Future;
use tokio::io::{ AsyncReadExt, AsyncWriteExt };
use bytes::BytesMut;
use std::collections::HashMap;
use std::sync::{ Arc, Mutex };
// use resp_async::ValueDecoder;
use resp::{ Decoder, Value };
use std::io::BufReader;

pub const DEFAULT_EXPIRY: u64 = 1000;

#[derive(Debug, PartialEq, Eq)]
pub enum RedisValue {
    String(String),
    List(Vec<String>),
    Hash(HashMap<String, String>),
}

impl RedisValue {
    fn from_string(value: String) -> RedisValue {
        RedisValue::String(value)
    }
    fn from_list(list: Vec<String>) -> RedisValue {
        RedisValue::List(list)
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

            // Set - return as array of bulk strings
            // RedisValue::Set(set) => {
            //     if set.is_empty() {
            //         "*0\r\n".to_string()
            //     } else {
            //         let mut response = format!("*{}\r\n", set.len());
            //         for item in set {
            //             response.push_str(&format!("${}\r\n{}\r\n", item.len(), item));
            //         }
            //         response
            //     }
            // }

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
        }
    }

    /// Returns null response when key doesn't exist
    pub fn get_null_response() -> String {
        "$-1\r\n".to_string()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    println!("Listening on 127.0.0.1:6379");

    // Shared database across all connections
    let database: Arc<Mutex<HashMap<String, RedisValue>>> = Arc::new(Mutex::new(HashMap::new()));

    loop {
        let (mut socket, _) = listener.accept().await?;
        let db_clone = Arc::clone(&database);

        tokio::spawn(async move {
            let mut buf = BytesMut::with_capacity(512);

            loop {
                let mut read_buf = [0u8; 512];
                match socket.read(&mut read_buf).await {
                    Ok(0) => {
                        break;
                    }
                    Ok(n) => {
                        buf.extend_from_slice(&read_buf[..n]);
                        let mut decoder = Decoder::new(BufReader::new(read_buf.as_slice()));
                        if let Ok(command_value) = decoder.decode() {
                            let command = Command::from_value(command_value);
                            let response = command.get_return(&db_clone).await;
                            let _ = socket.write_all(response.as_bytes()).await;

                            // Clear the buffer after successful decode
                            buf.clear();
                        }
                    }
                    Err(_) => {
                        break;
                    }
                }
            }
        });
    }
}

pub enum Command {
    PING,
    ECHO(String),
    SET(String, String),
    SetExpiry(String, String, String, String),
    GET(String),
    LPUSH(String, Vec<String>),
    RPUSH(String, Vec<String>),
    LRANGE(String, isize, isize),
    LLEN(String),
    UNKNOWN,
}

impl Command {
    pub fn from_value(value: Value) -> Command {
        match value {
            Value::Array(arr) if !arr.is_empty() => {
                // Extract string from the first Value
                if let Value::Bulk(command) = &arr[0] {
                    let cmd = command.clone();
                    match cmd.to_uppercase().as_str() {
                        "PING" => Command::PING,
                        "ECHO" if arr.len() > 1 => {
                            if let Value::Bulk(msg_bytes) = &arr[1] {
                                Command::ECHO(msg_bytes.clone())
                            } else {
                                Command::UNKNOWN
                            }
                        }
                        "SET" if arr.len() == 3 => {
                            if
                                let (Value::Bulk(key_bytes), Value::Bulk(val_bytes)) = (
                                    &arr[1],
                                    &arr[2],
                                )
                            {
                                Command::SET(key_bytes.clone(), val_bytes.clone())
                            } else {
                                Command::UNKNOWN
                            }
                        }
                        "SET" if arr.len() == 5 => {
                            if
                                let (
                                    Value::Bulk(key_bytes),
                                    Value::Bulk(val_bytes),
                                    Value::Bulk(expiry_command),
                                    Value::Bulk(timeout),
                                ) = (&arr[1], &arr[2], &arr[3], &arr[4])
                            {
                                Command::SetExpiry(
                                    key_bytes.clone(),
                                    val_bytes.clone(),
                                    expiry_command.clone(),
                                    timeout.clone()
                                )
                            } else {
                                Command::UNKNOWN
                            }
                        }
                        "RPUSH" if arr.len() > 1 => {
                            let mut value_array: Vec<String> = vec![];
                            if let Value::Bulk(key_bytes) = &arr[1] {
                                for (i, value) in arr.iter().enumerate() {
                                    if i > 1 {
                                        if let Value::Bulk(val) = value {
                                            println!("value parsed: {:?}", value);
                                            value_array.push(val.clone());
                                        } else {
                                            eprintln!("{:?}: was not parsed correctly", value);
                                        }
                                    }
                                }
                                return Command::RPUSH(key_bytes.clone(), value_array);
                            } else {
                                eprintln!("The key_bytes {:?}: was not parsed correctly", &arr[1]);
                            }

                            Command::UNKNOWN
                        }
                        "LPUSH" if arr.len() > 1 => {
                            let mut value_array: Vec<String> = vec![];
                            if let Value::Bulk(key_bytes) = &arr[1] {
                                for (i, value) in arr.iter().enumerate() {
                                    if i > 1 {
                                        if let Value::Bulk(val) = value {
                                            println!("value parsed: {:?}", value);
                                            value_array.push(val.clone());
                                        } else {
                                            eprintln!("{:?}: was not parsed correctly", value);
                                        }
                                    }
                                }
                                return Command::LPUSH(key_bytes.clone(), value_array);
                            } else {
                                eprintln!("The key_bytes {:?}: was not parsed correctly", &arr[1]);
                            }

                            // Command::LPUSH(, ())
                            Command::UNKNOWN
                        }
                        "LRANGE" if arr.len() > 1 => {
                            if
                                let (Value::Bulk(key_bytes), Value::Bulk(start), Value::Bulk(end)) =
                                    (&arr[1], &arr[2], &arr[3])
                            {
                                let start: isize = start.parse().unwrap();
                                let end: isize = end.parse().unwrap();
                                Command::LRANGE(key_bytes.clone(), start, end)
                            } else {
                                eprintln!("The values for LRANGE are not correctly formatted");
                                Command::UNKNOWN
                            }
                        }
                        "LLEN" if arr.len() > 1 => {
                            if let Value::Bulk(key_bytes) = &arr[1] {
                                Command::LLEN(key_bytes.clone())
                            } else {
                                Command::UNKNOWN
                            }
                        }
                        "GET" if arr.len() > 1 => {
                            if let Value::Bulk(key_bytes) = &arr[1] {
                                Command::GET(key_bytes.clone())
                            } else {
                                Command::UNKNOWN
                            }
                        }
                        _ => Command::UNKNOWN,
                    }
                } else {
                    Command::UNKNOWN
                }
            }
            _ => Command::UNKNOWN,
        }
    }

    pub async fn get_return(&self, database: &Arc<Mutex<HashMap<String, RedisValue>>>) -> String {
        match self {
            Command::PING => "+PONG\r\n".to_string(),
            Command::ECHO(msg) => format!("${}\r\n{}\r\n", msg.len(), msg),
            Command::SET(key, value) => {
                let mut db = database.lock().unwrap();
                let value = RedisValue::from_string(value.clone());
                db.insert(key.clone(), value);
                "+OK\r\n".to_string()
            }
            Command::SetExpiry(key, value, expiry_command, timeout) => {
                {
                    let mut db = database.lock().unwrap();
                    let value = RedisValue::from_string(value.clone());
                    db.insert(key.clone(), value);
                } // MutexGuard is dropped here

                // Clone the values before moving into spawned task
                let db_clone = Arc::clone(database);
                let key_clone = key.clone();
                let expiry_command_clone = expiry_command.clone();
                let timeout_clone = timeout.clone();

                tokio::spawn(async move {
                    let timeout: u64 = timeout_clone.parse().unwrap_or(DEFAULT_EXPIRY);
                    let expiry_time = match expiry_command_clone.as_str() {
                        "px" => Duration::from_millis(timeout),
                        "ex" => Duration::from_secs(timeout),
                        _ => {
                            eprintln!("GIVE A CORRECT TIMEOUT VALUE IDENTIFIER");
                            Duration::from_millis(DEFAULT_EXPIRY)
                        }
                    };

                    let now = Instant::now();
                    let later = now + expiry_time;

                    sleep_until(later).await;

                    let mut db = db_clone.lock().unwrap();
                    match db.remove(&key_clone) {
                        None => {
                            eprintln!("The key to be removed is not found in the database: {}", key_clone);
                        }
                        Some(x) => {
                            println!("Removed the Key: {:?} after expiry: {:?}", key_clone, x);
                        }
                    }
                });

                // Return OK immediately
                "+OK\r\n".to_string()
            }
            Command::LPUSH(key, list) => {
                let mut db = database.lock().unwrap();
                let final_list = if let Some(existing_value) = db.get(key) {
                    if let RedisValue::List(old_list) = existing_value {
                        // LPUSH prepends elements one by one from left to right
                        // So we need to reverse the new elements and prepend them
                        let mut final_list = list.clone();
                        final_list.reverse(); // Reverse the new elements
                        final_list.extend_from_slice(old_list);
                        final_list
                    } else {
                        // Key exists but is not a list - error in Redis
                        return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_string();
                    }
                } else {
                    // Key doesn't exist, create new list with reversed elements
                    let mut final_list = list.clone();
                    final_list.reverse();
                    final_list
                };

                db.insert(key.clone(), RedisValue::from_list(final_list.clone()));
                format!(":{}\r\n", final_list.len())
            }
            Command::RPUSH(key, list) => {
                let mut db = database.lock().unwrap();
                let final_list = if let Some(existing_value) = db.get(key) {
                    if let RedisValue::List(old_list) = existing_value {
                        // RPUSH appends to the end
                        let mut final_list = old_list.clone();
                        final_list.extend_from_slice(list);
                        final_list
                    } else {
                        // Key exists but is not a list - error in Redis
                        return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_string();
                    }
                } else {
                    // Key doesn't exist, create new list
                    list.clone()
                };

                db.insert(key.clone(), RedisValue::from_list(final_list.clone()));
                format!(":{}\r\n", final_list.len())
            }
            Command::LRANGE(key, start, end) => {
                let db = database.lock().unwrap();
                if let Some(msg) = db.get(key) {
                    // let slice =
                    if let RedisValue::List(list) = msg {
                        let slice = lrange_slice_vec(list, *start, *end);
                        let slice = RedisValue::from_list(slice);
                        let response = slice.get_response();
                        response
                    } else {
                        eprintln!("Error Fetching value from the database");
                        "*0\r\n".to_string()
                    }
                } else {
                    "*0\r\n".to_string()
                }
            }
            Command::GET(key) => {
                let db = database.lock().unwrap();
                if let Some(msg) = db.get(key) {
                    let response = msg.get_response();
                    response
                } else {
                    "$-1\r\n".to_string()
                }
            }
            Command::LLEN(key) => {
                let db = database.lock().unwrap();
                if let Some(msg) = db.get(key) {
                    if let RedisValue::List(list) = msg {
                        format!(":{}\r\n", list.len())
                    } else {
                        ":0\r\n".to_string()
                    }
                }else{
                    ":0\r\n".to_string()
                }
            }
            Command::UNKNOWN => "-ERR unknown command\r\n".to_string(),
        }
    }
}

pub fn lrange_slice_vec(list: &Vec<String>, start: isize, stop: isize) -> Vec<String> {
    let len = list.len() as isize;
    if len == 0 {
        return Vec::new();
    }

    // Convert negative indices to positive (counting from end)
    let mut actual_start = if start < 0 { len + start } else { start };
    let mut actual_stop = if stop < 0 { len + stop } else { stop };

    // Clamp indices to bounds
    if actual_start < 0 {
        actual_start = 0;
    }
    if actual_stop >= len {
        actual_stop = len - 1;
    }

    if actual_start > actual_stop || actual_start >= len {
        return Vec::new();
    }

    // Inclusive end index, exclusive for Rust slice
    let st = actual_start as usize;
    let en = (actual_stop + 1) as usize;
    list[st..en.min(list.len())].to_vec()
}
