#![allow(unused_imports)]
use std::net::TcpListener;
use std::io::Write;
use std::io::Read;
use std::net::TcpStream;
use std::error::Error;
fn main() {
    println!("Logs:");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");
                match handle_stream(&mut stream) {
                    Ok(()) => {}
                    Err(e) => {
                        println!("Error: {}", e);
                    }
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_stream(stream: &mut TcpStream) -> Result<(), Box<dyn Error>> {
    let mut buf = [0; 512];
    loop {
        stream.read(&mut buf)?;
        let read_count = stream.read(&mut buf)?;
        if read_count == 0 {
            break;
        }
        println!("wrote PONG");
        stream.write(b"+PONG\r\n")?;
    }
    Ok(())
}
