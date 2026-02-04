use std::io;

#[tokio::main]
async fn main() -> io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use walrus::Connection;
    use walrus::frame::Frame;

    async fn ping(connection: &mut Connection) -> Result<Option<Frame>, walrus::Error> {
        let frame_to_write = Frame::Simple(String::from("PING"));
        connection.write_frame(&frame_to_write).await.unwrap();
        connection.read_frame().await
    }

    use tokio::net::TcpStream;
    #[tokio::test]
    async fn ping_test() {
        let socket = TcpStream::connect("127.0.0.1:6379").await.unwrap();
        let mut connection = Connection::new(socket, Some(32));
        let frame = ping(&mut connection).await.unwrap().unwrap();

        assert_eq!(frame, Frame::Simple(String::from("PONG")));
    }

    #[tokio::test]
    async fn multi_ping_test() {
        let socket = TcpStream::connect("127.0.0.1:6379").await.unwrap();
        let mut connection = Connection::new(socket, Some(32));

        let mut frame_list = vec![];
        for _ in 0..5 {
            frame_list.push(ping(&mut connection).await.unwrap().unwrap());
        }

        let pong_frame = Frame::Simple(String::from("PONG"));
        for frame in frame_list.iter() {
            assert_eq!(*frame, pong_frame);
        }
    }
}
