#![allow(unused_imports)]
use std::error::Error;
use std::ops::Index;
use std::vec;
use tokio::net::TcpListener;
use tokio::time::{ sleep_until, Instant, Duration, interval };
use std::future::Future;
use tokio::io::{ AsyncReadExt, AsyncWriteExt };
use bytes::BytesMut;
use std::collections::HashMap;
use std::sync::{ Arc, Mutex };
// use resp_async::ValueDecoder;
use resp::{ Decoder, Value };
use std::io::BufReader;
pub const DEFAULT_EXPIRY: u64 = 1000;
use crate::value::{ RedisValue, StreamEntry };
pub enum Command {
    PING,
    ECHO(String),
    SET(String, String),
    SetExpiry(String, String, String, String),
    GET(String),
    TYPE(String),
    LPUSH(String, Vec<String>),
    RPUSH(String, Vec<String>),
    LRANGE(String, isize, isize),
    LLEN(String),
    LPOP(String, Option<isize>),
    BLPOP(String, f64),
    XADD(String, String, HashMap<String, String>),
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
                        "XADD" if arr.len() >= 5 => {
                            if
                                let (Value::Bulk(key_bytes), Value::Bulk(entry_id_bytes)) = (
                                    &arr[1],
                                    &arr[2],
                                )
                            {
                                if (arr.len() - 3) % 2 != 0 {
                                    return Command::UNKNOWN;
                                }
                                let mut field_pairs = HashMap::new();
                                let mut idx = 3;
                                while idx + 1 < arr.len() {
                                    match (&arr[idx], &arr[idx + 1]) {
                                        (Value::Bulk(field), Value::Bulk(val)) => {
                                            field_pairs.insert(field.clone(), val.clone());
                                            idx += 2;
                                        }
                                        _ => {
                                            return Command::UNKNOWN;
                                        }
                                    }
                                }
                                Command::XADD(
                                    key_bytes.clone(),
                                    entry_id_bytes.clone(),
                                    field_pairs
                                )
                            } else {
                                Command::UNKNOWN
                            }
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
                        "LPOP" if arr.len() == 2 => {
                            if let Value::Bulk(key_bytes) = &arr[1] {
                                Command::LPOP(key_bytes.clone(), None)
                            } else {
                                Command::UNKNOWN
                            }
                        }
                        "LPOP" if arr.len() > 2 => {
                            if
                                let (Value::Bulk(key_bytes), Value::Bulk(to_remove)) = (
                                    &arr[1],
                                    &arr[2],
                                )
                            {
                                let to_remove = to_remove.parse().unwrap();
                                Command::LPOP(key_bytes.clone(), Some(to_remove))
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
                        "TYPE" if arr.len() > 1 => {
                            if let Value::Bulk(key_bytes) = &arr[1] {
                                Command::TYPE(key_bytes.clone())
                            } else {
                                Command::UNKNOWN
                            }
                        }
                        "BLPOP" if arr.len() > 2 => {
                            if
                                let (Value::Bulk(key_bytes), Value::Bulk(timeout)) = (
                                    &arr[1],
                                    &arr[2],
                                )
                            {
                                let timeout: f64 = timeout.parse().unwrap_or(0.0);
                                Command::BLPOP(key_bytes.clone(), timeout)
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
                let expiry_command_clone = expiry_command.clone().to_lowercase();
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
            Command::XADD(key, entry_id, field_pairs) => {
                let key = key.clone();
                let entry_id = entry_id.clone();
                let field_pairs = field_pairs.clone();
                let new_entry = StreamEntry::from(entry_id.clone(), field_pairs.clone());
                let mut db = database.lock().unwrap();
                match db.get_mut(&key) {
                    Some(existing_value) => {
                        if let RedisValue::Stream(entries) = existing_value {
                            // entries.push(new_entry.clone());
                            let last_entry = entries.last();
                            match StreamEntry::validate_entry_id(new_entry.clone(), last_entry) {
                                None => {
                                    return "-ERR The ID specified in XADD must be greater than 0-0\r\n".to_string()
                                }
                                Some(result) => {
                                    match result {
                                        true => {
                                            // return "".to_string()
                                            entries.push(new_entry.clone());
                                        }
                                        false => {
                                            return "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n".to_string();
                                        }
                                    }
                                }
                            }
                        } else {
                            return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_string();
                        }
                    }
                    None => {
                        db.insert(key.clone(), RedisValue::from_stream(vec![new_entry]));
                    }
                }
                format!("${}\r\n{}\r\n", entry_id.len(), entry_id)
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
            Command::TYPE(key) => {
                let db = database.lock().unwrap();
                if let Some(msg) = db.get(key) {
                    let response = msg.get_type_response();
                    response
                } else {
                    "+none\r\n".to_string()
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
                } else {
                    ":0\r\n".to_string()
                }
            }
            Command::LPOP(key, to_remove) => {
                let mut db = database.lock().unwrap();
                if let Some(msg) = db.get(key) {
                    if let RedisValue::List(list) = msg {
                        // format!(":{}\r\n", list.len())
                        let mut popped_list = list.clone();
                        let mut response = "".to_string();
                        if let Some(index) = *to_remove {
                            if index > (popped_list.len() as isize) {
                                eprintln!("Error: Index exceeds the mentioned value");
                                return "$-1\r\n".to_string();
                            }
                            let mut response_list: Vec<String> = vec![];
                            for i in 0..index {
                                let popped_element = popped_list.remove(0);
                                response_list.push(popped_element);
                            }
                            response = RedisValue::from_list(response_list).get_response();
                        } else {
                            let popped_element = popped_list.remove(0);
                            response = RedisValue::from_string(popped_element).get_response();
                        }
                        db.insert(key.clone(), RedisValue::from_list(popped_list.clone()));
                        response
                    } else {
                        "$-1\r\n".to_string()
                    }
                } else {
                    "$-1\r\n".to_string()
                }
            }
            Command::BLPOP(key, timeout_duration) => {
                let db_clone = Arc::clone(database);
                let key_clone = key.clone();
                let timeout = *timeout_duration;

                // First check if there's already an element available
                {
                    let mut db = db_clone.lock().unwrap();
                    if let Some(redis_value) = db.get_mut(&key_clone) {
                        if let RedisValue::List(list) = redis_value {
                            if !list.is_empty() {
                                let popped_element = list.remove(0);
                                // Return array with key name and popped element
                                return format!(
                                    "*2\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                                    key_clone.len(),
                                    key_clone,
                                    popped_element.len(),
                                    popped_element
                                );
                            }
                        }
                    }
                }

                // If no element available, start blocking
                let start_time = Instant::now();
                let mut interval = interval(Duration::from_millis(10)); // Check every 10ms
                loop {
                    interval.tick().await;

                    // Check if timeout reached (if timeout > 0)
                    if timeout > 0.0 {
                        // dbg!("ENTERS HERE");
                        let elapsed = start_time.elapsed().as_secs() as f64;
                        dbg!(elapsed);
                        if elapsed >= timeout {
                            return "*-1\r\n".to_string(); // Timeout reached
                        }
                    }

                    // Check if element is now available
                    let mut db = db_clone.lock().unwrap();
                    if let Some(redis_value) = db.get_mut(&key_clone) {
                        if let RedisValue::List(list) = redis_value {
                            if !list.is_empty() {
                                let popped_element = list.remove(0);
                                // Return array with key name and popped element
                                return format!(
                                    "*2\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                                    key_clone.len(),
                                    key_clone,
                                    popped_element.len(),
                                    popped_element
                                );
                            }
                        }
                    }
                    // Drop the lock to allow other operations to modify the list
                    drop(db);
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
