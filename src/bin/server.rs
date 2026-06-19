use clap::Parser;
use tokio::io::{self};
use tokio::net::TcpListener;
use walrus::server;

#[derive(Parser)]
#[command(version, about, long_about= None)]
struct Args {
    /// Optionally take port from the user.
    #[arg(short, long)]
    port: Option<i16>,
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let args = Args::parse();
    let port = match args.port {
        Some(port) => port,
        // Default port
        None => 6380,
    };

    let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).await?;
    server::run(listener, port).await;
    Ok(())
}
