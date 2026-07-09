use clap::Parser;
use tokio::io::{self};
use tokio::net::TcpListener;
use walrus::server;

#[cfg(not(target_env = "msvc"))]
use jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[derive(Parser)]
#[command(version, about, long_about= None)]
struct Args {
    /// Optionally take port from the user.
    #[arg(short, long, help = "Sets the port to use for the server.")]
    port: Option<i16>,
    /// Optionally take initial read buffer size in KB from the user.
    #[arg(
        short,
        long = "read-buffer-size",
        help = "Sets the initial read buffer size for the server in KB."
    )]
    read_buffer_size: Option<u16>,
    /// Optionally take initial write buffer size in KB from the user.
    #[arg(
        short,
        long = "write-buffer-size",
        help = "Sets the initial write buffer size for the server in KB."
    )]
    write_buffer_size: Option<u16>,
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let args = Args::parse();
    let port = match args.port {
        Some(port) => port,
        // Default port
        None => 6380,
    };
    let read_buffer_size = args.read_buffer_size;
    let write_buffer_size = args.write_buffer_size;

    let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).await?;

    server::run(listener, port, read_buffer_size, write_buffer_size).await;
    Ok(())
}
