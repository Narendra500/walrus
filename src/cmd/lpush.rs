use std::collections::VecDeque;

use bytes::Bytes;

use crate::{
    Connection,
    db::{Data, Db},
    errors::WalrusError,
    frame::Frame,
    parse::Parse,
};

/// Push a `Data` item at the start of the list with the key `list_key`.
pub struct LPush {
    list_key: Bytes,
    /// Array containing the data to be pushed to the list.
    data: VecDeque<Data>,
}

impl LPush {
    /// Create a new `LPush` command which pushes the data to the start of list
    /// with key `list_key`.
    pub fn new(list_key: Bytes, data: VecDeque<Data>) -> LPush {
        LPush { list_key, data }
    }

    /// Parse a `LPush` instance from an array frame.
    /// The LPush string is already consumed.
    /// Returns the `LPush` instance on success or error if frame is malformed.
    ///
    /// Expects an array containg atleast 3 entries.
    /// LPush list_key array_of_items_to_push
    pub(crate) fn parse_frames(parse: &mut Parse) -> Result<LPush, WalrusError> {
        let list_key = parse.next_bytes()?;
        let value = parse.next_array()?;
        Ok(LPush {
            list_key,
            data: value,
        })
    }

    /// Execute the `LPush` command, pushing the array items in self.data to the start of the array
    /// corresponding to `list_key` key.
    ///
    /// Returns the number of data elements in the array after insertion if successful or
    /// `WRONGTYPE` error if data item with `list_key` is not a list.
    pub(crate) async fn execute(self, db: &Db, conn: &mut Connection) -> Result<(), WalrusError> {
        let key = self.list_key.clone();

        // Use block to strictly scope the DashMap lock.
        // The return either Ok(len) or Err(()), this determines the network response.
        let result = {
            if let Some(mut entry) = db.get_mut(&key) {
                // Key exists.
                match &mut entry.data {
                    Data::Array(list) => {
                        let new_data = self.data;
                        for data in new_data {
                            list.push_front(data);
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
            Err(_) => {
                conn.write_error_frame(WalrusError::WrongType.get_msg());
            }
        }

        Ok(())
    }

    /// Convert `LPush` instance to `Frame` consuming self.
    /// Will `panic` if `self.data` contains nested arrays.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("lpush"));
        frame.push_bulk(self.list_key);
        frame.push_data(self.data);

        frame
    }
}
