// #![allow(unused_imports)]
// use std::net::TcpListener;
use std::io::Write;
use std::io::Read;
// use std::net::TcpStream;
use std::error::Error;
use tokio::net::TcpListener;
use tokio::io::{ AsyncReadExt, AsyncWriteExt };

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    println!("Listening on 127.0.0.1:6379");

    loop {
        let (mut socket, _) = listener.accept().await?;
        tokio::spawn(async move {
            let mut buf = [0; 512];
            loop {
                match socket.read(&mut buf).await {
                    Ok(0) => {
                        break;
                    }
                    Ok(_) => {
                        let _ = socket.write_all(b"+PONG\r\n").await;
                        println!("Pong Sent");
                    }
                    Err(_) => {
                        break;
                    }
                }
            }
        });
    }
}
