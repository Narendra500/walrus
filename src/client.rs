use std::{collections::VecDeque, time::Duration};

use bytes::Bytes;
use tokio::net::{TcpStream, ToSocketAddrs};

use crate::{
    Connection,
    cmd::{BLPop, Get, LLen, LPop, LPush, LRange, Ping, RPush, Set, Type},
    db::Data,
    errors::WalrusError,
    frame::Frame,
};

/// Contains the connection established with the `walrus` server.
pub struct Client {
    /// TCP stream wrapped in `Connection`, which provides frame parsing.
    connection: Connection,
}

pub fn int_to_string(val: i64) -> String {
    let mut buf = itoa::Buffer::new();
    let printed = buf.format(val);
    printed.to_string()
}

pub fn double_to_string(val: f64) -> String {
    use ryu;
    let mut buffer = ryu::Buffer::new();
    let printed: &str = buffer.format(val);
    printed.to_string()
}

impl Client {
    /// Establish a connection with Walrus server at `addr`.
    ///
    /// The `addr` passed must be of type that can be asynchronously converted to `SocketAddr`.
    pub async fn connect<T: ToSocketAddrs>(
        addr: T,
        read_buffer_size: Option<u16>,
        write_buffer_size: Option<u16>,
    ) -> Result<Client, WalrusError> {
        let socket = TcpStream::connect(addr).await?;
        let connection = Connection::new(socket, read_buffer_size, write_buffer_size);
        Ok(Client { connection })
    }

    /// Send `Ping` command to the server.
    ///
    /// Returns the message provided if any given the server is running.
    pub async fn ping(&mut self, msg: Option<Bytes>) -> Result<Bytes, WalrusError> {
        let frame = Ping::new(msg).into_frame();
        self.connection.write_frame(&frame);

        if let Some(response) = self.connection.read_frame().await? {
            match response {
                Frame::Simple(value) => Ok(Bytes::from(value)),
                Frame::Bulk(value) => Ok(value),
                Frame::Error(err) => Err(err.into()),
                _ => Err("Invalid response by server".into()),
            }
        } else {
            Err("No response from server".into())
        }
    }

    /// `Get` the `value` associated with the `key`
    pub async fn get(&mut self, key: Bytes) -> Result<Option<Bytes>, WalrusError> {
        let frame = Get::new(key).into_frame();
        self.connection.write_frame(&frame);

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
        key: Bytes,
        value: Bytes,
        expire: Option<Duration>,
    ) -> Result<Bytes, WalrusError> {
        let frame = Set::new(key, value, expire).into_frame();
        self.connection.write_frame(&frame);

        if let Some(response) = self.connection.read_frame().await? {
            match response {
                Frame::Bulk(value) => Ok(value),
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
        list_key: Bytes,
        data: VecDeque<Data>,
    ) -> Result<i64, WalrusError> {
        let frame = RPush::new(list_key, data).into_frame();
        self.connection.write_frame(&frame);

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
        list_key: Bytes,
        data: VecDeque<Data>,
    ) -> Result<i64, WalrusError> {
        let frame = LPush::new(list_key, data).into_frame();
        self.connection.write_frame(&frame);

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
        list_key: Bytes,
        count: Option<i64>,
    ) -> Result<Option<Vec<Data>>, WalrusError> {
        let frame = LPop::new(list_key, count).into_frame();
        self.connection.write_frame(&frame);

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

    /// `BLPop` command to remove and return the first element of the first non empty list
    /// with key in the order specified by the keys argument.
    ///
    /// Will block until one or more keys are available to pop from.
    ///
    /// #Arguments
    /// - keys: list of keys to pop from
    /// - timeout: timeout in seconds.
    ///
    /// A timeout of 0 can be used to block indefinitely.
    ///
    /// #Returns
    /// Array with first element being the name of the key that was popped and second element
    /// being the value of the key.
    /// `None` if timeout was reached or if none of the keys were found.
    pub async fn blpop(
        &mut self,
        keys: Vec<Bytes>,
        timeout: f64,
    ) -> Result<Option<Vec<Data>>, WalrusError> {
        let frame = BLPop::new(keys, timeout).into_frame();
        self.connection.write_frame(&frame);

        if let Some(response) = self.connection.read_frame().await? {
            match response {
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
    pub async fn llen(&mut self, list_key: Bytes) -> Result<i64, WalrusError> {
        let frame = LLen::new(list_key).into_frame();
        self.connection.write_frame(&frame);

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
    /// Returns array of `Data` items if successful else `WalrusError` is returned.
    pub async fn lrange(
        &mut self,
        list_key: Bytes,
        start_index: i64,
        end_index: i64,
    ) -> Result<Vec<Data>, WalrusError> {
        let frame = LRange::new(list_key, start_index, end_index).into_frame();
        self.connection.write_frame(&frame);

        if let Some(response) = self.connection.read_frame().await? {
            match response {
                // Handles all types of frames.
                frame => Ok(Data::frame_to_data_vec(frame)?),
            }
        } else {
            Err("No response from server".into())
        }
    }

    /// `Type` command to get the type of the data associated with the given key.
    /// Returns the type of the data if successful.
    /// Returns "none" if the key doesn't exist.
    /// Returns "list" if the data associated with the key is a list.
    /// Returns "string" for Bytes, Integer, Double and String.
    /// Although Integer and Double are stored as i64 and f64 internally, the type
    /// presented is string.
    pub async fn wtype(&mut self, key: Bytes) -> Result<Bytes, WalrusError> {
        let frame = Type::new(key).into_frame();
        self.connection.write_frame(&frame);

        if let Some(response) = self.connection.read_frame().await? {
            match response {
                Frame::Simple(value) => Ok(Bytes::from(value)),
                Frame::Bulk(value) => Ok(value),
                Frame::Error(err) => Err(err.into()),
                _ => Err("Invalid response by server".into()),
            }
        } else {
            Err("No response from server".into())
        }
    }
}
