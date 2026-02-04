use tokio::io::{self};
use tokio::net::TcpListener;
use walrus::server;

#[tokio::main]
async fn main() -> io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    server::run(listener).await;
    Ok(())
}
