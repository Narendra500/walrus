use std::time::Duration;

use bytes::Bytes;
use tokio::net::{TcpStream, ToSocketAddrs};

use crate::{
    Connection,
    cmd::{Get, Ping, RPush, Set},
    db::Data,
    frame::Frame,
};

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

    /// `Get` the `value` associated with the `key`
    pub async fn get(&mut self, key: String) -> Result<Option<Bytes>, crate::Error> {
        let frame = Get::new(key).into_frame();
        self.connection.write_frame(&frame).await?;

        if let Some(response) = self.connection.read_frame().await? {
            match response {
                Frame::Simple(value) => Ok(Some(value.into())),
                Frame::Bulk(value) => Ok(Some(value)),
                // `Null` frame is sent by server, if key has no associated value.
                Frame::Null => Ok(None),
                Frame::Error(err) => Err(err.into()),
                _ => Err("Invalid response by server".into()),
            }
        } else {
            Err("No response from server".into())
        }
    }

    /// `Set` a value for the key. If key already exists it's previous value is replaced.
    /// Takes optional expiration duration.
    pub async fn set(
        &mut self,
        key: String,
        value: Bytes,
        expire: Option<Duration>,
    ) -> Result<String, crate::Error> {
        let frame = Set::new(key, value, expire).into_frame();
        self.connection.write_frame(&frame).await?;

        if let Some(response) = self.connection.read_frame().await? {
            match response {
                Frame::Simple(value) => Ok(value),
                Frame::Error(err) => Err(err.into()),
                _ => Err("Invalid response by server".into()),
            }
        } else {
            Err("No response from server".into())
        }
    }

    /// Append an array of `Data` elements to the end of the array with key `list_key`.
    /// Returns the number of elements in the array after append.
    /// If `data` given is not empty and the response is 0, then there exists no array
    /// with the key `list_key`.
    pub async fn rpush(&mut self, list_key: String, data: Vec<Data>) -> Result<u64, crate::Error> {
        let frame = RPush::new(list_key, data).into_frame();
        self.connection.write_frame(&frame).await?;

        if let Some(response) = self.connection.read_frame().await? {
            match response {
                Frame::Integer(value) => Ok(value),
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
    use std::time::Duration;

    use crate::client::Client;
    use crate::db::Data;
    use bytes::Bytes;
    use tokio::time::{Instant, sleep_until};

    #[tokio::test]
    async fn ping_test() {
        let mut client = Client::connect("127.0.0.1:6379").await.unwrap();
        let ping_response = client.ping(None).await.unwrap();

        assert_eq!(ping_response, Bytes::from("pong"));
    }

    #[tokio::test]
    async fn ping_test_with_message() {
        let message = "Hello There!".as_bytes();
        let mut client = Client::connect("127.0.0.1:6379").await.unwrap();
        let ping_response = client.ping(Some(Bytes::from(message))).await.unwrap();
        println!("{ping_response:?}");

        assert_eq!(ping_response, Bytes::from(message));
    }

    #[tokio::test]
    async fn multi_ping_test() {
        let mut client = Client::connect("127.0.0.1:6379").await.unwrap();

        let mut ping_response_list = vec![];
        for _ in 0..5 {
            ping_response_list.push(client.ping(None).await.unwrap());
        }

        let pong = String::from("pong");
        for response in ping_response_list.iter() {
            assert_eq!(*response, pong);
        }
    }

    #[tokio::test]
    async fn multi_ping_test_with_message() {
        let message = "Hello There!".as_bytes();
        let mut client = Client::connect("127.0.0.1:6379").await.unwrap();

        let mut ping_response_list = vec![];
        for _ in 0..5 {
            ping_response_list.push(client.ping(Some(Bytes::from(message))).await.unwrap());
        }

        let pong = Bytes::from(message);
        for response in ping_response_list.iter() {
            println!("{:?}", *response);
            assert_eq!(*response, pong);
        }
    }

    #[tokio::test]
    async fn set_test_no_expire() {
        let mut client = Client::connect("127.0.0.1:6379").await.unwrap();

        let key = "key1".to_string();
        let value = Bytes::from("value1 value2 value3 value4");

        let set_response = client.set(key, value, None).await.unwrap();

        assert_eq!("OK", set_response);
    }

    /// Sets a key value pair with 1000 millisecond expiration duration.
    /// Attempts to fetch teh value of the same key again after the key is expired.
    /// Expected response from server is a Null frame for the get command.
    #[tokio::test]
    async fn set_get_test_after_expire() {
        let mut client = Client::connect("127.0.0.1:6379").await.unwrap();

        let key = "key2".to_string();
        let value = Bytes::from("value1 value2 value3 value4");
        let expire = Duration::from_millis(1000);

        let now = Instant::now();
        let set_response = client.set(key.clone(), value, Some(expire)).await.unwrap();

        // OK is the expected response for successful set command
        assert_eq!("OK", set_response);

        // sleep until the key is expired.
        sleep_until(now + expire).await;
        let get_response = client.get(key.clone()).await.unwrap();

        // the response must be None.
        match get_response {
            None => {}
            Some(response) => {
                panic!("Invalid response from server: {response:?}");
            }
        }
    }

    /// Sets a key value pair with 1000 millisecond expiration.
    /// Attempts to fetch the value of the same key before the key expires.
    /// The expected response is a Bulk frame containing the value of the key.
    #[tokio::test]
    async fn set_get_test_before_expire() {
        let mut client = Client::connect("127.0.0.1:6379").await.unwrap();

        let key = "key3".to_string();
        let original_value = Bytes::from("value1 value2 value3 value4");
        let expire = Duration::from_millis(1000);

        let now = Instant::now();
        let set_response = client
            .set(key.clone(), original_value.clone(), Some(expire))
            .await
            .unwrap();

        // OK is the expected respones for successful set command.
        assert_eq!("OK", set_response);

        // If the key isn't expired yet attempt to fetch it.
        if Instant::now() < now + expire {
            let get_response = client.get(key).await.unwrap().unwrap();
            assert_eq!(get_response, original_value);
        } else {
            println!("The key expired before sending the get command.");
        }
    }

    #[tokio::test]
    async fn rpush_test() {
        let mut client = Client::connect("127.0.0.1:6379").await.unwrap();

        let list_key = String::from("list1");
        let data = vec![
            Data::String("val1".to_string()),
            Data::Integer(1),
            Data::Bytes(Bytes::from("val3")),
        ];

        let rpush_response = client.rpush(list_key, data).await.unwrap();
        println!("rpush_response: {rpush_response}");

        assert_ne!(rpush_response, 0);
    }
}
