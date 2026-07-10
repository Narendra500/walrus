use bytes::Bytes;

use crate::{
    Connection,
    db::{Data, Db},
    errors::WalrusError,
    frame::Frame,
};

/// LPop command to remove and return the first `count` elements of the list with key
/// with key `list_key`.
/// If `count` is negative, the value out of range error is returned.
/// If `count` is zero, an empty array is returned.
/// If the list is empty or doesn't exist, `Frame::Null` is returned.
/// If `count` is greater than length of the list, the count is clamped to the length of the list.
pub struct LPop {
    list_key: Bytes,
    count: i64,
}

impl LPop {
    /// Return a new LPop command.
    pub fn new(list_key: Bytes, count: Option<i64>) -> Self {
        if let Some(count) = count {
            Self { list_key, count }
        }
        // If count is not given, then default to 1.
        else {
            Self { list_key, count: 1 }
        }
    }

    /// Parse the Lpop command from an array frame.
    /// The 'LPOP' string is already consumed.
    /// Returns Ok(Self) if successful.
    /// Returns Err(Error) if parsing fails.
    ///
    /// The array frame must have atleast 2 elements.
    /// LPOP list_key <count>
    pub(crate) fn parse_frames(parse: &mut crate::parse::Parse) -> Result<Self, WalrusError> {
        let list_key = parse.next_bytes()?;
        // If count was not given then default of 1 is taken.
        let count = parse.next_int().unwrap_or(1);
        Ok(Self::new(list_key, Some(count)))
    }

    /// Execute the LPop command.
    /// Writes the first `count` elements of the list with key `list_key` to the client connection if successful.
    /// Writes `Frame::Null` if the list is empty or doesn't exist.
    /// Writes Empty array if `count` is zero.
    /// Returns `Value out of range` error if `count` is negative.
    pub(crate) async fn execute(&self, db: &Db, conn: &mut Connection) -> Result<(), WalrusError> {
        let key = &self.list_key;
        if let Some(mut entry) = db.get_mut(key) {
            match &mut entry.data {
                Data::Array(list) => {
                    let len = list.len() as i64;
                    let mut count = self.count;
                    // Clamp count to the length of the list.
                    count = count.min(len);

                    // If count is negative, then return an error.
                    if count < 0 {
                        conn.write_error_frame("value is out of range, must be positive");
                    } else if count == 0 {
                        // If count is zero, then return an empty array.
                        conn.write_data_array(vec![].into_iter(), 0);
                    } else if count == 1 {
                        // unwrap is safe as we clamp count to the length of the list.
                        // Return single element as a single frame instead of an array.
                        conn.write_data(&list.pop_front().unwrap());
                    } else {
                        conn.write_data_array(list.range(0..count as usize), count as usize);
                        list.drain(0..count as usize);
                    }
                }
                // Data associated with the given key is not a list.
                _ => conn.write_error_frame(WalrusError::WrongType.get_msg()),
            }
        }
        // No Data associated with the given key.
        else {
            conn.write_null_frame();
        }

        Ok(())
    }

    /// Convert `LPop` instance to `Frame`.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("lpop"));
        frame.push_bulk(self.list_key);
        frame.push_int(self.count);

        frame
    }
}
