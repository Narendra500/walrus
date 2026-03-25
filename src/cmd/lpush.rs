use std::collections::VecDeque;

use crate::{
    Connection,
    db::{Data, Db},
    frame::Frame,
    parse::Parse,
};

/// Push a `Data` item at the start of the list with the key `list_key`.
pub struct LPush {
    list_key: String,
    /// Array containing the data to be pushed to the list.
    data: VecDeque<Data>,
}

impl LPush {
    /// Create a new `LPush` command which pushes the data to the start of list
    /// with key `list_key`.
    pub fn new(list_key: impl ToString, data: VecDeque<Data>) -> LPush {
        LPush {
            list_key: list_key.to_string(),
            data,
        }
    }

    /// Parse a `LPush` instance from an array frame.
    /// The LPush string is already consumed.
    /// Returns the `LPush` instance on success or error if frame is malformed.
    ///
    /// Expects an array containg atleast 3 entries.
    /// LPush list_key array_of_items_to_push
    pub(crate) fn parse_frames(parse: &mut Parse) -> Result<LPush, crate::Error> {
        let list_key = parse.next_string()?;
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
    pub(crate) async fn execute(self, db: &Db, conn: &mut Connection) -> Result<(), crate::Error> {
        // Get the db data corresponding to the `list_key`
        let maybe_list = db.get(&self.list_key);
        // If data with `list_key` exists in db.
        if let Some(list) = maybe_list {
            match list {
                Data::Array(mut list) => {
                    // Take ownership of the data in self.
                    let mut data = self.data;
                    // Reverse the data in self, to maintain redis behaviour (where the last item
                    // in the input becomes the new first item of the list).
                    data.make_contiguous().reverse();
                    // Append the list data in db to data. Effectively pushing the data to the start.
                    data.append(&mut list);
                    // Point the list in db to the new data.
                    list = data;
                    let list_len = list.len();
                    let frame = Frame::Integer(list_len as i64);
                    conn.write_frame(&frame).await.unwrap();
                }
                // The data corresponding to `list_key` is not an array.
                _ => {
                    conn.write_frame(&Frame::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                    ))
                    .await?;
                }
            }
        }
        // No data with corresponding to `list_key` key in db. so create one.
        else {
            // Extract key out of self.
            let key = self.list_key;
            // Extract data out of self. This leaves self empty.
            let mut data = self.data;
            // Reverse the data in self, to maintain redis behaviour (where the last item
            // in the input becomes the new first item of the list).
            data.make_contiguous().reverse();
            // Get the length of the data before it is moved into the db.
            let list_len = data.len();
            db.set(key, Data::Array(data), None);
            // Return the length of array.
            let frame = Frame::Integer(list_len as i64);
            conn.write_frame(&frame).await.unwrap();
        }

        Ok(())
    }

    /// Convert `LPush` instance to `Frame` consuming self.
    /// Will `panic` if `self.data` contains nested arrays.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_string(String::from("lpush"));
        frame.push_string(self.list_key);
        frame.push_data(self.data);

        frame
    }
}
