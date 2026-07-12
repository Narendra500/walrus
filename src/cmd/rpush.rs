use std::collections::VecDeque;

use bytes::Bytes;

use crate::{
    Connection,
    db::{Data, Db},
    errors::WalrusError,
    frame::Frame,
    parse::Parse,
};

pub(crate) enum RPushData {
    Frames {
        frames: Vec<Frame>,
        start_pos: usize,
    },
    Data(VecDeque<Data>),
}

/// Push a `Data` item into the list with the key `list_key`.
pub struct RPush {
    list_key: Bytes,
    /// Array containing the data to be appended to the list.
    data: RPushData,
}

impl RPush {
    /// Create a new `RPush` command which pushes the data to the end of list
    /// with key `list_key`.
    pub fn new(list_key: Bytes, data: VecDeque<Data>) -> RPush {
        RPush {
            list_key,
            data: RPushData::Data(data),
        }
    }

    /// Parse a `RPush` instance from an array frame.
    /// The RPush string is already consumed.
    /// Returns the `RPush` instance on success or error if frame is malformed.
    ///
    /// Expects an array containg atleast 3 entries.
    /// RPush list_key array_of_items_to_push
    pub(crate) fn parse_frames(parse: &mut Parse) -> Result<RPush, WalrusError> {
        let list_key = parse.next_bytes()?;
        let (frames, pos) = parse.take_parts();
        Ok(RPush {
            list_key,
            data: RPushData::Frames {
                frames,
                start_pos: pos,
            },
        })
    }

    /// Execute the `RPush` command, appending the array items in self.data to array
    /// corresponding to `list_key` key.
    ///
    /// Returns the number of data elements in the array after insertion if successful or
    /// `WRONGTYPE` error if data item with `list_key` is not a list.
    pub(crate) async fn execute(self, db: &Db, conn: &mut Connection) -> Result<(), WalrusError> {
        let key = self.list_key;

        match self.data {
            RPushData::Frames {
                mut frames,
                start_pos,
            } => {
                if let Some(mut entry) = db.get_mut(&key) {
                    match &mut entry.data {
                        Data::Array(list) => {
                            for frame in frames.drain(start_pos..) {
                                list.push_back(
                                    Data::try_from(frame).map_err(|e| WalrusError::Internal(e))?,
                                );
                            }
                            conn.write_data(&Data::Integer(list.len() as i64));
                        }
                        _ => conn.write_error_frame(WalrusError::WrongType.get_msg()),
                    }
                } else {
                    let mut list = VecDeque::with_capacity(frames.len() - start_pos);
                    for frame in frames.drain(start_pos..) {
                        list.push_back(
                            Data::try_from(frame).map_err(|e| WalrusError::Internal(e))?,
                        );
                    }
                    let list_len = list.len();
                    db.set(&key, Data::Array(list), None);
                    conn.write_data(&Data::Integer(list_len as i64));
                    db.notify_blocked(&key);
                }
            }
            RPushData::Data(mut new_data) => {
                if let Some(mut entry) = db.get_mut(&key) {
                    // Key exists.
                    match &mut entry.data {
                        Data::Array(list) => {
                            list.append(&mut new_data);
                            conn.write_data(&Data::Integer(list.len() as i64));
                        }
                        // Not an array.
                        _ => conn.write_error_frame(WalrusError::WrongType.get_msg()),
                    }
                } else {
                    // Key doesn't exist, create it.
                    let list_len = new_data.len();

                    db.set(&key, Data::Array(new_data), None);

                    conn.write_data(&Data::Integer(list_len as i64));
                    db.notify_blocked(&key);
                }
            }
        }

        Ok(())
    }

    /// Convert `RPush` instance to `Frame` consuming self.
    /// Will `panic` if `self.data` contains nested arrays.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("rpush"));
        frame.push_bulk(self.list_key);
        match self.data {
            RPushData::Data(data) => frame.push_data(data),
            RPushData::Frames {
                mut frames,
                start_pos,
            } => {
                for f in frames.drain(start_pos..) {
                    frame.push(f);
                }
            }
        }

        frame
    }
}
