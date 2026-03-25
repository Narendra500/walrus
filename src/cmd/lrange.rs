use crate::{
    Connection,
    db::{Data, Db},
    frame::Frame,
    parse::Parse,
};

/// `LRange` Command to fetch elements of a list from some start offset
/// to end offset (both inclusive).
/// Offsets can be negative (e.g,. -1 is last element, -2 is penultimate and so on).
pub struct LRange {
    list_key: String,
    /// The starting offset (inclusive).
    /// Can be negative (e.g,. -1 for the last element).
    start_index: i64,
    /// The ending offset (inclusive).
    /// Can be negative.
    end_index: i64,
}

impl LRange {
    /// Returns a `LRange` instance.
    /// Takes key which can be of any datatype that implements `ToString`, a start_index and
    /// end_index both i64.
    pub fn new(list_key: impl ToString, start_index: i64, end_index: i64) -> LRange {
        LRange {
            list_key: list_key.to_string(),
            start_index,
            end_index,
        }
    }

    /// Parse a `LRange` instance from an array frame.
    /// The 'LRange' String is already consumed.
    /// Returns the `LRange` instance on success or error if frame is malformed.
    ///
    /// Expects an array containing 4 entries.
    /// LRANGE list_key start_index end_index
    pub(crate) fn parse_frame(parse: &mut Parse) -> Result<LRange, crate::Error> {
        let list_key = parse.next_string()?;
        let start_index = parse.next_int()?;
        let end_index = parse.next_int()?;

        Ok(LRange {
            list_key,
            start_index,
            end_index,
        })
    }

    /// Execute the `LRange` command, the data from the section of the list requested is cloned
    /// and sent to the client by writing the response to the `conn`.
    pub(crate) async fn execute(self, db: &Db, conn: &mut Connection) -> Result<(), crate::Error> {
        let maybe_list = db.get(&self.list_key);

        if let Some(list) = maybe_list {
            match list {
                Data::Array(list) => {
                    let len = list.len() as i64;
                    // Convert negative start index to positive. Say len is 5, then -1 bceomes 4
                    // -2 becomes 3 and so on.
                    let mut start_index = if self.start_index < 0 {
                        len + self.start_index
                    } else {
                        self.start_index
                    };
                    // Convert negative end index to positive.
                    let mut end_index = if self.end_index < 0 {
                        len + self.end_index
                    } else {
                        self.end_index
                    };

                    // If abs(start_index) was greater then length of the list, then it would still
                    // be negative at this point. But the actual list starting from index 0 is
                    // still overlapping partially with the requested list. So we bound start index
                    // to 0.
                    start_index = std::cmp::max(0, start_index);
                    // If end index is greater than len of the list, then we bound it to len - 1 as
                    // that overlaps with the requested list of greater size.
                    end_index = std::cmp::min(len - 1, end_index);

                    // The portion of the list requested is empty.
                    if start_index > end_index || start_index >= len {
                        conn.write_frame(&Frame::Array(vec![])).await?;
                        return Ok(());
                    }

                    // Wrap Vec<Frame> into Frame::Array.
                    let frame = Frame::Array(
                        // Collect the data items in a Vec<Frame>.
                        // NOTE: If Data contains an array then it will be flattened due to
                        // Frame::from()'s current implementation.
                        list.range(start_index as usize..=end_index as usize)
                            .cloned()
                            .map(|data| Frame::from(data))
                            .collect::<Vec<Frame>>(),
                    );

                    conn.write_frame(&frame).await?;
                }
                // Data associated with the given key is not a list.
                _ => {
                    conn.write_frame(&Frame::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                    ))
                    .await?;
                }
            }
        }
        // No list with given key.
        else {
            conn.write_frame(&Frame::Array(vec![])).await?;
        }

        Ok(())
    }

    /// Convert `LRange` instance to `Frame`.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_string("lrange".into());
        frame.push_string(self.list_key);
        frame.push_int(self.start_index);
        frame.push_int(self.end_index);

        frame
    }
}
