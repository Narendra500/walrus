use std::collections::VecDeque;

use crate::{
    Connection,
    db::{Data, Db},
    frame::Frame,
    parse::Parse,
};

/// Push a `Data` item into the list with the key `list_key`.
pub struct RPush {
    list_key: String,
    /// Array containing the data to be appended to the list.
    data: VecDeque<Data>,
}

impl RPush {
    /// Create a new `RPush` command which pushes the data to the end of list
    /// with key `list_key`.
    pub fn new(list_key: impl ToString, data: VecDeque<Data>) -> RPush {
        RPush {
            list_key: list_key.to_string(),
            data,
        }
    }

    /// Parse a `RPush` instance from an array frame.
    /// The RPush string is already consumed.
    /// Returns the `RPush` instance on success or error if frame is malformed.
    ///
    /// Expects an array containg atleast 3 entries.
    /// RPush list_key array_of_items_to_push
    pub(crate) fn parse_frames(parse: &mut Parse) -> Result<RPush, crate::Error> {
        let list_key = parse.next_string()?;
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
    pub(crate) async fn execute(
        mut self,
        db: &Db,
        conn: &mut Connection,
    ) -> Result<(), crate::Error> {
        // Get the db data corresponding to the `list_key`
        let maybe_list = db.get(&self.list_key);
        // If data with `list_key` exists in db.
        if let Some(list) = maybe_list {
            match list {
                Data::Array(mut list) => {
                    let data = &mut self.data;
                    list.append(data);
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
            let vec_data = self.data;
            let vec_deque_data = VecDeque::from(vec_data);
            // Get the length of the data before it is moved into the db.
            let list_len = vec_deque_data.len();
            db.set(key, Data::Array(vec_deque_data), None);
            // Return the length of array.
            let frame = Frame::Integer(list_len as i64);
            conn.write_frame(&frame).await.unwrap();
        }

        Ok(())
    }

    /// Convert `RPush` instance to `Frame` consuming self.
    /// Will `panic` if `self.data` contains nested arrays.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_string(String::from("rpush"));
        frame.push_string(self.list_key);
        frame.push_data(self.data);

        frame
    }
}
