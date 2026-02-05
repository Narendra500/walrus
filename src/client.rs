use bytes::Bytes;
use tokio::net::{TcpStream, ToSocketAddrs};

use crate::{Connection, cmd::Ping, frame::Frame};

/// Contains the connection established with the `walrus` server.
pub struct Client {
    /// TCP stream wrapped in `Connection`, which provides frame parsing.
    connection: Connection,
}

impl Client {
    /// Establish a connection with Walrus server at `addr`.
    ///
    /// The `addr` passed must be of type that can be asynchronously converted to `SocketAddr`.
    pub async fn connect<T: ToSocketAddrs>(addr: T) -> Result<Client, crate::Error> {
        let socket = TcpStream::connect(addr).await?;
        let connection = Connection::new(socket, Some(32));
        Ok(Client { connection })
    }

    /// Send `Ping` command to the server.
    ///
    /// Returns the message provided if any given the server is running.
    pub async fn ping(&mut self, msg: Option<Bytes>) -> Result<Bytes, crate::Error> {
        let frame = Ping::new(msg).into_frame();
        self.connection.write_frame(&frame).await?;

        if let Some(response) = self.connection.read_frame().await? {
            match response {
                Frame::Simple(value) => Ok(value.into()),
                Frame::Bulk(value) => Ok(value),
                Frame::Error(err) => Err(err.into()),
                _ => Err("Invalid response by server".into()),
            }
        } else {
            Err("No response from server".into())
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::Connection;
    use crate::cmd::Ping;
    use crate::frame::Frame;
    use bytes::Bytes;
    use tokio::net::TcpStream;

    async fn ping(
        connection: &mut Connection,
        msg: Option<Bytes>,
    ) -> Result<Option<Frame>, crate::Error> {
        let ping_frame = Ping::new(msg).into_frame();
        connection.write_frame(&ping_frame).await.unwrap();
        connection.read_frame().await
    }

    #[tokio::test]
    async fn ping_test() {
        let socket = TcpStream::connect("127.0.0.1:6379").await.unwrap();
        let mut connection = Connection::new(socket, Some(32));
        let frame = ping(&mut connection, None).await.unwrap().unwrap();

        assert_eq!(frame, Frame::Simple(String::from("pong")));
    }

    #[tokio::test]
    async fn ping_test_with_message() {
        let message = "Hello There!".as_bytes();
        let socket = TcpStream::connect("127.0.0.1:6379").await.unwrap();
        let mut connection = Connection::new(socket, Some(32));
        let frame = ping(&mut connection, Some(Bytes::from(message)))
            .await
            .unwrap()
            .unwrap();
        println!("{frame}");

        assert_eq!(frame, Frame::Bulk(Bytes::from(message)));
    }

    #[tokio::test]
    async fn multi_ping_test() {
        let socket = TcpStream::connect("127.0.0.1:6379").await.unwrap();
        let mut connection = Connection::new(socket, Some(32));

        let mut frame_list = vec![];
        for _ in 0..5 {
            frame_list.push(ping(&mut connection, None).await.unwrap().unwrap());
        }

        let pong_frame = Frame::Simple(String::from("pong"));
        for frame in frame_list.iter() {
            assert_eq!(*frame, pong_frame);
        }
    }

    #[tokio::test]
    async fn multi_ping_test_with_message() {
        let message = "Hello There!".as_bytes();
        let socket = TcpStream::connect("127.0.0.1:6379").await.unwrap();
        let mut connection = Connection::new(socket, Some(32));

        let mut frame_list = vec![];
        for _ in 0..5 {
            frame_list.push(
                ping(&mut connection, Some(Bytes::from(message)))
                    .await
                    .unwrap()
                    .unwrap(),
            );
        }

        let pong_frame = Frame::Bulk(Bytes::from(message));
        for frame in frame_list.iter() {
            println!("{}", *frame);
            assert_eq!(*frame, pong_frame);
        }
    }
}
