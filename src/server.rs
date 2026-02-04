use crate::Command;
use crate::connection::Connection;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio::time;

/// Tcp listening and initialization of per-connection state.
struct Listener {
    listener: TcpListener,
    /// Limit the max number of connections.
    /// A `Semaphore` is used to limit the max number of connections. Permit is required
    /// from semaphore before attempting to accept a new connection. Must wait for one
    /// if none are available.
    ///
    /// Permit is returned to semaphore when connection is dropped.
    limit_connections: Arc<Semaphore>,
}

/// Per connection handler. Reads requests from `connection` and applies commands.
struct Handler {
    connection: Connection,
}

const MAX_CONNECTIONS: usize = 1000;

/// Run the server.
///
/// Accepts connections from the listener given as argument.
/// A task is spawned is to handle each connection.
pub async fn run(listener: TcpListener) {
    // Create a listener state instance.
    let mut server = Listener {
        listener,
        limit_connections: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
    };

    // Run the server, accepting inbound connections.
    server.run().await.unwrap();
}

impl Listener {
    async fn run(&mut self) -> Result<(), crate::Error> {
        println!("Accepting inbound connections at port 6379");
        loop {
            // Get a permit to accept the connection ensuring number of active connections
            // don't exceed `MAX_CONNECTIONS`.
            // Wait if permit not available immediately.
            // `acquire_owned` returns error when the semaphore has been closed, which is
            // never the case here so `unwrap` is safe.
            let permit = self
                .limit_connections
                .clone()
                .acquire_owned()
                .await
                .unwrap();

            // Since `accept` attempts error handling by itself, an error here is not
            // recoverable.
            let socket = self.accept().await?;

            // Per connection handler.
            let mut handler = Handler {
                connection: Connection::new(socket, Some(32)),
            };

            // Spawn a new task to process the connection.
            tokio::spawn(async move {
                // Process the connection, prints error if any.
                if let Err(err) = handler.run().await {
                    println!("connection error, {err}");
                }
                // Drop the permit after the task is completed, returning the permit back to
                // the semaphore.
                drop(permit);
            });
        }
    }

    /// Accept inbound connection.
    ///
    /// On success TcpStream is returned, else the execution of accept is paused for
    /// 1 second, then 2 seconds after second failed accept and so on doubling until
    /// 64 seconds. After 6th failed attempt to accept, an error is returned.
    async fn accept(&mut self) -> Result<TcpStream, crate::Error> {
        // Initial sleep time if accept fails.
        let mut sleep_time = 1;

        // Accept loop
        loop {
            match self.listener.accept().await {
                Ok((socket, _)) => return Ok(socket),
                Err(err) => {
                    if sleep_time > 64 {
                        // Failed too many times, return error.
                        return Err(err.into());
                    }
                }
            }

            // Pause execution for atleast `sleep_time` seconds.
            time::sleep(Duration::from_secs(sleep_time)).await;

            // Double the `sleep_time`.
            sleep_time *= 2;
        }
    }
}

impl Handler {
    async fn run(&mut self) -> Result<(), crate::Error> {
        loop {
            // Try to read a frame from the socket.
            let frame = match self.connection.read_frame().await? {
                Some(frame) => frame,
                // Peer closed the connection. Nothing to do further.
                None => return Ok(()),
            };

            let cmd = Command::from_frame(frame)?;

            cmd.execute(&mut self.connection).await?;
        }
    }
}
