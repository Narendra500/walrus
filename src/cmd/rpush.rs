use std::collections::VecDeque;

use bytes::Bytes;

use crate::{
    Connection,
    db::{Data, Db},
    errors::WalrusError,
    frame::Frame,
    parse::Parse,
};

/// Push a `Data` item into the list with the key `list_key`.
pub struct RPush {
    list_key: Bytes,
    /// Array containing the data to be appended to the list.
    data: VecDeque<Data>,
}

impl RPush {
    /// Create a new `RPush` command which pushes the data to the end of list
    /// with key `list_key`.
    pub fn new(list_key: Bytes, data: VecDeque<Data>) -> RPush {
        RPush { list_key, data }
    }

    /// Parse a `RPush` instance from an array frame.
    /// The RPush string is already consumed.
    /// Returns the `RPush` instance on success or error if frame is malformed.
    ///
    /// Expects an array containg atleast 3 entries.
    /// RPush list_key array_of_items_to_push
    pub(crate) fn parse_frames(parse: &mut Parse) -> Result<RPush, WalrusError> {
        let list_key = parse.next_bytes()?;
        let value = parse.next_array()?;
        Ok(RPush {
            list_key,
            data: value,
        })
    }

    /// Execute the `RPush` command, appending the array items in self.data to array
    /// corresponding to `list_key` key.
    ///
    /// Returns the number of data elements in the array after insertion if successful or
    /// `WRONGTYPE` error if data item with `list_key` is not a list.
    pub(crate) async fn execute(self, db: &Db, conn: &mut Connection) -> Result<(), WalrusError> {
        let key = self.list_key;

        // Use block to strictly scope the DashMap lock.
        // The return either Ok(len) or Err(()), this determines the network response.
        let result = {
            if let Some(mut entry) = db.get_mut(&key) {
                // Key exists.
                match &mut entry.data {
                    Data::Array(list) => {
                        let new_data = self.data;
                        for data in new_data {
                            list.push_back(data);
                        }
                        Ok(list.len())
                    }
                    // Not an array.
                    _ => Err(()),
                }
            } else {
                // Key doesn't exist, create it.
                let mut new_data = self.data;
                new_data.make_contiguous().reverse();
                let list_len = new_data.len();

                db.set(&key, Data::Array(new_data), None);

                Ok(list_len)
            }
        }; // Dashmap lock is dropped here.

        match result {
            Ok(len) => {
                let data = &Data::Integer(len as i64);
                conn.write_data(data);
                db.notify_blocked(&key);
            }
            Err(_) => conn.write_error_frame(WalrusError::WrongType.get_msg()),
        }

        Ok(())
    }

    /// Convert `RPush` instance to `Frame` consuming self.
    /// Will `panic` if `self.data` contains nested arrays.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("rpush"));
        frame.push_bulk(self.list_key);
        frame.push_data(self.data);

        frame
    }
}
