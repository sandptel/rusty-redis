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
pub mod command;
pub mod value;
pub const DEFAULT_EXPIRY: u64 = 1000;
use command::Command;
use value::RedisValue;

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



