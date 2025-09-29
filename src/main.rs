#![allow(unused_imports)]
use std::error::Error;
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    println!("Listening on 127.0.0.1:6379");

    // Shared database across all connections
    let database: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::new()));

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
    SET_EXPIRY(String, String, String, String),
    GET(String),
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
                                Command::SET_EXPIRY(
                                    key_bytes.clone(),
                                    val_bytes.clone(),
                                    expiry_command.clone(),
                                    timeout.clone()
                                )
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

    /// Returns a future that completes when the specified expiry `Instant` is reached.
    // pub async fn wait_until_expiry(
    //     database: &Arc<Mutex<HashMap<String, String>>>,
    //     expiry: Instant,
    //     key: &String
    // ) -> () {
    //     sleep_until(expiry);
    //     let mut db = database.lock().unwrap();
    //     // db.insert(key.clone(), value.clone());
    //     match db.remove(key) {
    //         None => { eprintln!("The key to be removed is not found in the database: {key}") }
    //         Some(x) => { println!("Removed the Key: {key} after expiry : {x}") }
    //     }
    // }
    pub async fn get_return(&self, database: &Arc<Mutex<HashMap<String, String>>>) -> String {
        match self {
            Command::PING => "+PONG\r\n".to_string(),
            Command::ECHO(msg) => format!("${}\r\n{}\r\n", msg.len(), msg),
            Command::SET(key, value) => {
                let mut db = database.lock().unwrap();
                db.insert(key.clone(), value.clone());

                "+OK\r\n".to_string()
            }
            Command::SET_EXPIRY(key, value, expiry_command, timeout) => {
                {
                    let mut db = database.lock().unwrap();
                    db.insert(key.clone(), value.clone());
                } // MutexGuard is dropped here

                let timeout: u64 = timeout.parse().unwrap();
                let expiry_time = match expiry_command.as_str() {
                    "PX" => { Duration::from_millis(timeout) }
                    "EX" => { Duration::from_secs(timeout) }
                    _ => {
                        eprintln!("GIVE A CORRECT TIMEOUT VALUE IDENTIFIER");
                        Duration::from_millis(DEFAULT_EXPIRY)
                    }
                };
                let now = Instant::now();
                let later = now + expiry_time;

                sleep_until(later).await;

                {
                    let mut db = database.lock().unwrap();
                    match db.remove(key) {
                        None => {
                            eprintln!("The key to be removed is not found in the database: {key}");
                        }
                        Some(x) => {
                            println!("Removed the Key: {key} after expiry : {x}");
                        }
                    }
                } // MutexGuard is dropped here

                "+OK\r\n".to_string()
            }
            Command::GET(key) => {
                let db = database.lock().unwrap();
                if let Some(msg) = db.get(key) {
                    format!("${}\r\n{}\r\n", msg.len(), msg)
                } else {
                    "$-1\r\n".to_string()
                }
            }
            Command::UNKNOWN => "-ERR unknown command\r\n".to_string(),
        }
    }
}
