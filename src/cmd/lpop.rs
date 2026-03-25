use crate::{
    Connection,
    db::{Data, Db},
    frame::Frame,
};

/// LPop command to remove and return the first `count` elements of the list with key
/// with key `list_key`.
/// If `count` is negative, the value out of range error is returned.
/// If `count` is zero, an empty array is returned.
/// If the list is empty or doesn't exist, `Frame::Null` is returned.
/// If `count` is greater than length of the list, the count is clamped to the length of the list.
pub struct LPop {
    list_key: String,
    count: i64,
}

impl LPop {
    /// Return a new LPop command.
    pub fn new(list_key: String, count: Option<i64>) -> Self {
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
    pub(crate) fn parse_frames(parse: &mut crate::parse::Parse) -> Result<Self, crate::Error> {
        let list_key = parse.next_string()?;
        let count = parse.next_int()?;
        // If count was not given then default of 1 would have been sent by the client.
        Ok(Self::new(list_key, Some(count)))
    }

    /// Execute the LPop command.
    /// Writes the first `count` elements of the list with key `list_key` to the client connection if successful.
    /// Writes `Frame::Null` if the list is empty or doesn't exist.
    /// Writes Empty array if `count` is zero.
    /// Returns `Value out of range` error if `count` is negative.
    pub(crate) async fn execute(&self, db: &Db, conn: &mut Connection) -> Result<(), crate::Error> {
        let maybe_list = db.get(&self.list_key);
        if let Some(list) = maybe_list {
            match list {
                Data::Array(mut list) => {
                    let len = list.len() as i64;
                    let mut count = self.count;
                    // If count is negative, then return an error.
                    if count < 0 {
                        conn.write_frame(&Frame::Error(String::from("ERR value is out of range")))
                            .await?;
                    }
                    // If count is zero, then return an empty array.
                    if count == 0 {
                        conn.write_frame(&Frame::Array(vec![])).await?;
                        return Ok(());
                    }
                    // Clamp count to the length of the list.
                    count = count.min(len);

                    // Return single element as a single frame instead of an array.
                    if count == 1 {
                        // unwrap is safe as we clamp count to the length of the list.
                        let response = Frame::from(list.pop_front().unwrap());
                        conn.write_frame(&response).await?;
                    } else {
                        // Prepare the response array.
                        let response = Frame::Array(
                            list.drain(0..count as usize)
                                .map(|data| Frame::from(data))
                                .collect::<Vec<Frame>>(),
                        );
                        conn.write_frame(&response).await?;
                    }
                }
                // Data associated with the given key is not a list.
                _ => {
                    conn.write_frame(&Frame::Null).await?;
                }
            }
        }
        // No Data associated with the given key.
        else {
            conn.write_frame(&Frame::Null).await?;
        }

        Ok(())
    }

    /// Convert `LPop` instance to `Frame`.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_string("lpop".into());
        frame.push_string(self.list_key);
        frame.push_int(self.count);

        frame
    }
}
