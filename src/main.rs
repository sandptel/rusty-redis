#![allow(unused_imports)]
use std::error::Error;
use tokio::net::TcpListener;
use tokio::io::{ AsyncReadExt, AsyncWriteExt };
use bytes::BytesMut;
// use resp_async::ValueDecoder;
use resp::{ Decoder, Value };
use std::io::BufReader;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    println!("Listening on 127.0.0.1:6379");

    loop {
        let (mut socket, _) = listener.accept().await?;
        tokio::spawn(async move {
            // let mut decoder = ValueDecoder::default();
            // let mut buf = BytesMut::with_capacity(512);
            let mut read_buf = [0u8; 512];

            loop {
                match socket.read(&mut read_buf).await {
                    Ok(0) => {
                        break;
                    }
                    Ok(n) => {
                        // buf.extend_from_slice(&read_buf[..n]);
                        let mut decoder = Decoder::new(BufReader::new(read_buf.as_slice()));
                        if let Ok(command_value) = decoder.decode() {
                            let command = Command::from_value(command_value);
                            let response = command.get_return();
                            let _ = socket.write_all(response.as_bytes()).await;
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
                    match cmd.as_str() {
                        "PING" => Command::PING,
                        "ECHO" if arr.len() > 1 => {
                            if let Value::Bulk(msg_bytes) = &arr[1] {
                                Command::ECHO(msg_bytes.clone())
                            } else {
                                Command::UNKNOWN
                            }
                        }
                        "SET" if arr.len() > 2 => {
                            if let (Value::Bulk(key_bytes), Value::Bulk(val_bytes)) = (&arr[1], &arr[2]) {
                                Command::SET(
                                    key_bytes.clone(),
                                    val_bytes.clone()
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

    pub fn get_return(&self) -> String {
        match self {
            Command::PING => "+PONG\r\n".to_string(),
            Command::ECHO(msg) => format!("${}\r\n{}\r\n", msg.len(), msg),
            Command::SET(_, _) => "+OK\r\n".to_string(),
            Command::GET(_) => "$-1\r\n".to_string(), // Not implemented, returns nil
            Command::UNKNOWN => "-ERR unknown command\r\n".to_string(),
        }
    }
}
