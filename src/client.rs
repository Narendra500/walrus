use std::{collections::VecDeque, time::Duration};

use bytes::Bytes;
use tokio::net::{TcpStream, ToSocketAddrs};

use crate::{
    Connection,
    cmd::{Get, LLen, LPop, LPush, LRange, Ping, RPush, Set},
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
    pub async fn connect<T: ToSocketAddrs>(
        addr: T,
        capacity: Option<usize>,
    ) -> Result<Client, crate::Error> {
        let socket = TcpStream::connect(addr).await?;
        let connection = Connection::new(socket, capacity);
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
    /// `WRONGTYPE` error is returned when the given key is not a list.
    pub async fn rpush(
        &mut self,
        list_key: String,
        data: VecDeque<Data>,
    ) -> Result<i64, crate::Error> {
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

    /// Push an array of `Data` elements to the start of the array with key `list_key`.
    /// Returns the number of elements in the array after push operatoin.
    /// `WRONGTYPE` error is returned when the given key is not a list.
    /// The last element of the data becomes the first element of the list.
    /// So \[1, 2 ,3\] becomes \[3, 2, 1, ...existing elements in the list\].
    pub async fn lpush(
        &mut self,
        list_key: String,
        data: VecDeque<Data>,
    ) -> Result<i64, crate::Error> {
        let frame = LPush::new(list_key, data).into_frame();
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

    /// `LPop` command to remove and return the first `count` elements of the list with key
    /// `list_key`.
    /// Returns the list the of first `count` (clamped to the length of the list) elements of the
    /// list if successful.
    /// Returns `Frame::Null` if the list with `list_key` is empty, or list doesn't exist.
    /// Returns 'Value of out range' error if `count` is negative.
    pub async fn lpop(
        &mut self,
        list_key: String,
        count: Option<i64>,
    ) -> Result<Option<Vec<Data>>, crate::Error> {
        let frame = LPop::new(list_key, count).into_frame();
        self.connection.write_frame(&frame).await?;

        if let Some(response) = self.connection.read_frame().await? {
            match response {
                // Frame::Null case throws error in the frame_to_data_vec function as `Data`
                // doesn't support `Null` values.
                Frame::Null => Ok(None),
                value => Ok(Some(Data::frame_to_data_vec(value)?)),
            }
        } else {
            Err("No response from server".into())
        }
    }

    /// `LLen` command to get the length of a list.
    /// Returns the length of the list if successful or `WRONGTYPE` error if data item with
    /// `list_key` is not a list.
    /// Returns `0` if no list with `list_key` is found.
    pub async fn llen(&mut self, list_key: impl ToString) -> Result<i64, crate::Error> {
        let frame = LLen::new(list_key).into_frame();
        self.connection.write_frame(&frame).await?;

        if let Some(response) = self.connection.read_frame().await? {
            match response {
                Frame::Integer(value) => Ok(value),
                Frame::Error(err) => Err(err.into()),
                _ => Err("Invalid response by server".into()),
            }
        } else {
            Err("No response by server".into())
        }
    }

    /// Fetchs items of list with key `list_key` in the range \[`start_index`, `end_index`\].
    /// Any item in the range will be returned even if the entire range doesn't overlap with the
    /// list boundries.
    /// If `start_index` is negative and abs(`start_index`) > list.len() then `start_index` will
    /// be bound to 0.
    /// If `end_index` > list.len() it will be bound to list.len() - 1.
    /// `start_index` > `end_index` or `start_index` >= list.lend() will return an empty array.
    ///
    /// Returns array of `Data` items if successful else `Error` is returned.
    pub async fn lrange(
        &mut self,
        list_key: String,
        start_index: i64,
        end_index: i64,
    ) -> Result<Vec<Data>, crate::Error> {
        let frame = LRange::new(list_key, start_index, end_index).into_frame();
        self.connection.write_frame(&frame).await?;

        if let Some(response) = self.connection.read_frame().await? {
            match response {
                // Handles all types of frames.
                frame => Ok(Data::frame_to_data_vec(frame)?),
            }
        } else {
            Err("No response from server".into())
        }
    }
}

#[cfg(test)]
mod tests {
    use rand::{RngExt, distr::Alphanumeric, random};
    use std::{collections::VecDeque, time::Duration};

    use crate::client::Client;
    use crate::db::Data;
    use bytes::Bytes;
    use tokio::time::{Instant, sleep_until};

    const SERVER_IPADDRESS: &str = "127.0.0.1:6379";

    fn random_string(len: usize) -> String {
        rand::rng()
            .sample_iter(&Alphanumeric)
            .take(len)
            .map(char::from)
            .collect()
    }

    fn random_data_array(len: usize) -> VecDeque<Data> {
        let data_type: Vec<Data> = vec![
            Data::String("".into()),
            Data::Integer(0),
            Data::Bytes("".into()),
        ];

        let mut data_vec = VecDeque::with_capacity(len);

        for _ in 0..len {
            let data_type_index = (random::<u64>() % data_type.len() as u64) as usize;
            let data_type = data_type[data_type_index].clone();
            let data = match data_type {
                Data::String(_) => Data::String(random_string(6)),
                Data::Integer(_) => Data::Integer(random::<i64>()),
                Data::Bytes(_) => Data::Bytes(Bytes::from(random_string(6))),
                _ => unreachable!(),
            };
            data_vec.push_back(data);
        }

        data_vec
    }

    #[tokio::test]
    async fn ping_test() {
        let mut client = Client::connect(SERVER_IPADDRESS.to_string(), Some(32))
            .await
            .unwrap();
        let ping_response = client.ping(None).await.unwrap();

        assert_eq!(ping_response, Bytes::from("pong"));
    }

    #[tokio::test]
    async fn ping_test_with_message() {
        let message = "Hello There!".as_bytes();
        let mut client = Client::connect(SERVER_IPADDRESS.to_string(), Some(32))
            .await
            .unwrap();
        let ping_response = client.ping(Some(Bytes::from(message))).await.unwrap();
        println!("{ping_response:?}");

        assert_eq!(ping_response, Bytes::from(message));
    }

    #[tokio::test]
    async fn multi_ping_test() {
        let mut client = Client::connect(SERVER_IPADDRESS.to_string(), Some(32))
            .await
            .unwrap();

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
        let mut client = Client::connect(SERVER_IPADDRESS.to_string(), Some(32))
            .await
            .unwrap();

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
        let mut client = Client::connect(SERVER_IPADDRESS.to_string(), Some(32))
            .await
            .unwrap();

        let key = random_string(6);
        let value = Bytes::from("value1 value2 value3 value4");

        let set_response = client.set(key, value, None).await.unwrap();

        assert_eq!("OK", set_response);
    }

    /// Sets a key value pair with 1000 millisecond expiration duration.
    /// Attempts to fetch teh value of the same key again after the key is expired.
    /// Expected response from server is a Null frame for the get command.
    #[tokio::test]
    async fn set_get_test_after_expire() {
        let mut client = Client::connect(SERVER_IPADDRESS.to_string(), Some(32))
            .await
            .unwrap();

        let key = random_string(6);
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
        let mut client = Client::connect(SERVER_IPADDRESS.to_string(), Some(32))
            .await
            .unwrap();

        let key = random_string(6);
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

    /// Pushes a list containing Data into server db.
    /// Checks if the response is not zero.
    #[tokio::test]
    async fn rpush_test() {
        let mut client = Client::connect(SERVER_IPADDRESS.to_string(), Some(32))
            .await
            .unwrap();

        let list_key = random_string(6);
        let data = random_data_array(3);
        let len = data.len() as i64;

        let rpush_response = client.rpush(list_key, data).await.unwrap();
        println!("rpush_response: {rpush_response}");

        assert_eq!(rpush_response, len as i64);
    }

    /// Creates a list with key `list_key` and then pushes another list to the front of the list.
    /// Checks if the length of the list is the sum of the two lists.
    #[tokio::test]
    async fn lpush_test() {
        let mut client = Client::connect(SERVER_IPADDRESS.to_string(), Some(32))
            .await
            .unwrap();

        let list_key = random_string(6);
        let data = random_data_array(3);
        let len = data.len() as i64;

        let rpush_response = client.rpush(list_key.clone(), data).await.unwrap();
        assert_eq!(rpush_response, len);

        let data2 = VecDeque::from([
            Data::String(random_string(6)),
            Data::Integer(random::<i64>()),
            Data::Bytes(Bytes::from(random_string(6))),
        ]);
        let len2 = data2.len() as i64;
        let lpush_response = client.lpush(list_key.clone(), data2).await.unwrap();

        assert_eq!(lpush_response, len + len2);
    }

    /// Pushes a list to the server db and then requests the full list back.
    /// checks if the returned list has same elements as the one sent originally.
    /// start is 0 and end is length of list - 1.
    #[tokio::test]
    async fn lrange_test_full_range() {
        let mut client = Client::connect(SERVER_IPADDRESS.to_string(), Some(32))
            .await
            .unwrap();

        let list_key = random_string(6);
        let data = random_data_array(3);
        let len = data.len() as i64;

        // Send data to create the list with.
        let rpush_response = client.rpush(list_key.clone(), data.clone()).await.unwrap();
        println!("rpush_response: {rpush_response}");

        assert_eq!(rpush_response, len);

        // Get back all elements of the list.
        let start_index = 0;
        let end_index = -1;
        let lrange_response = client
            .lrange(list_key, start_index, end_index)
            .await
            .unwrap();

        assert_eq!(data, lrange_response);
    }

    /// Pushes a list to the server db and then requests the full list back.
    /// checks if the returned list has same elements as the one sent originally.
    /// start is -(length of list * 2) this ensures that the final value of start is
    /// negative and end is length of list. This ensures that the requested range is superset of
    /// the actual list range.
    #[tokio::test]
    async fn lrange_out_of_bounds_test() {
        let mut client = Client::connect(SERVER_IPADDRESS.to_string(), Some(32))
            .await
            .unwrap();

        let list_key = random_string(6);
        let mut data = random_data_array(3);
        let len = data.len() as i64;

        // Send data to create the list with.
        let rpush_response = client.lpush(list_key.clone(), data.clone()).await.unwrap();
        println!("rpush_response: {rpush_response}");

        assert_eq!(rpush_response, len);

        // Get back all elements of the list.
        let start_index = -(len * 2);
        let end_index = len;
        let lrange_response = client
            .lrange(list_key, start_index, end_index)
            .await
            .unwrap();
        println!("lrange_response: {lrange_response:?}");

        data.make_contiguous().reverse();
        assert_eq!(data, lrange_response);
    }

    /// Get's back the last two elements of the list using negative indices.
    #[tokio::test]
    async fn lrange_test_negative_indices() {
        let mut client = Client::connect(SERVER_IPADDRESS, Some(32)).await.unwrap();
        let list_key = random_string(8);

        let data = VecDeque::from([Data::Integer(1), Data::Integer(2), Data::Integer(3)]);

        client.rpush(list_key.clone(), data).await.unwrap();

        // Get the last two elements using negative indices [-2, -1]
        let res = client.lrange(list_key, -2, -1).await.unwrap();

        assert_eq!(res.len(), 2);
        assert_eq!(res[0], Data::Integer(2));
        assert_eq!(res[1], Data::Integer(3));
    }

    /// Pushes a list to the server db and then requests the length of the list.
    /// checks if the returned length is same as the one sent originally.
    #[tokio::test]
    async fn llen_test() {
        let mut client = Client::connect(SERVER_IPADDRESS.to_string(), Some(32))
            .await
            .unwrap();

        let list_key = random_string(6);
        let data = random_data_array(3);
        let len = data.len() as i64;

        // Send data to create the list with.
        let rpush_response = client.rpush(list_key.clone(), data).await.unwrap();
        println!("rpush_response: {rpush_response}");

        assert_eq!(rpush_response, len);

        // Get back the length of the list.
        let llen_response = client.llen(list_key).await.unwrap();
        assert_eq!(llen_response, len);
    }

    /// Test for `LPop` command, returns the first element of the list with key.
    #[tokio::test]
    async fn lpop_test_first_only() {
        let mut client = Client::connect(SERVER_IPADDRESS.to_string(), Some(32))
            .await
            .unwrap();

        let list_key = random_string(6);
        let data = random_data_array(6);

        // Send data to create the list with.
        let rpush_response = client.rpush(list_key.clone(), data.clone()).await.unwrap();
        println!("rpush_response: {rpush_response}");

        assert_eq!(rpush_response, data.len() as i64);

        // Get back the first element of the list.
        let count = 1;
        let lpop_response = client.lpop(list_key, Some(count)).await.unwrap().unwrap();
        println!("lpop_response: {lpop_response:?}");

        assert_eq!(
            lpop_response,
            data.range(0..count as usize).cloned().collect::<Vec<_>>()
        );
    }

    /// Test for `LPop` command, returns the first `count` elements of the list with key.
    #[tokio::test]
    async fn lpop_test_multiple_within_bounds() {
        let mut client = Client::connect(SERVER_IPADDRESS.to_string(), Some(32))
            .await
            .unwrap();

        let list_key = random_string(6);
        let data = random_data_array(6);

        // Send data to create the list with.
        let rpush_response = client.rpush(list_key.clone(), data.clone()).await.unwrap();
        println!("rpush_response: {rpush_response}");

        assert_eq!(rpush_response, data.len() as i64);

        let count = data.len() as i64 - 1;
        let lpop_response = client.lpop(list_key, Some(count)).await.unwrap().unwrap();
        println!("lpop_response: {lpop_response:?}");

        assert_eq!(
            lpop_response,
            data.range(0..count as usize).cloned().collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    #[should_panic(expected = "ERR value is out of range")]
    async fn lpop_test_negative_count() {
        let mut client = Client::connect(SERVER_IPADDRESS.to_string(), Some(32))
            .await
            .unwrap();

        let list_key = random_string(6);
        let data = random_data_array(6);

        // Send data to create the list with.
        let rpush_response = client.rpush(list_key.clone(), data.clone()).await.unwrap();
        println!("rpush_response: {rpush_response}");

        assert_eq!(rpush_response, data.len() as i64);

        let count = -1;
        // Panics with "Value out of range" error.
        let _ = client.lpop(list_key, Some(count)).await.unwrap();
    }
}
