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

enum LPopErrors {
    ValueOutOfRange,
    WrongType,
    NotFound,
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
        let key = self.list_key.clone();
        let result = {
            if let Some(mut entry) = db.get_mut(&key) {
                match &mut entry.data {
                    Data::Array(list) => {
                        let res: Result<Frame, LPopErrors>;
                        let len = list.len() as i64;
                        let mut count = self.count;
                        // Clamp count to the length of the list.
                        count = count.min(len);

                        // If count is negative, then return an error.
                        if count < 0 {
                            res = Err(LPopErrors::ValueOutOfRange);
                        } else if count == 0 {
                            // If count is zero, then return an empty array.
                            res = Ok(Frame::Array(vec![]));
                        } else if count == 1 {
                            // unwrap is safe as we clamp count to the length of the list.
                            let response = Frame::from(list.pop_front().unwrap());
                            // Return single element as a single frame instead of an array.
                            res = Ok(response);
                        } else {
                            // Prepare the response array.
                            let response = Frame::Array(
                                list.drain(0..count as usize)
                                    .map(|data| Frame::from(data))
                                    .collect::<Vec<Frame>>(),
                            );
                            res = Ok(response);
                        }
                        res
                    }
                    // Data associated with the given key is not a list.
                    _ => Err(LPopErrors::WrongType),
                }
            }
            // No Data associated with the given key.
            else {
                Err(LPopErrors::NotFound)
            }
        }; // Dashmap lock dropped here.

        match result {
            Ok(frame) => {
                conn.write_frame(&frame).await?;
            }
            Err(err) => match err {
                LPopErrors::ValueOutOfRange => {
                    conn.write_frame(&Frame::Error(
                        "value is out of range, must be positive".into(),
                    ))
                    .await?;
                }
                LPopErrors::NotFound => {
                    conn.write_frame(&Frame::Null).await?;
                }
                LPopErrors::WrongType => {
                    conn.write_frame(&Frame::Error(WalrusError::WrongType.into()))
                        .await?;
                }
            },
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
