use crate::{
    Connection,
    db::{Data, Db},
    frame::Frame,
    parse::Parse,
};

/// `LLen` command to get the length of a list.
pub struct LLen {
    list_key: String,
}

impl LLen {
    /// Returns a `LLen` instance.
    /// Takes key which can be of any datatype that implements `ToString`.
    pub fn new(list_key: impl ToString) -> LLen {
        LLen {
            list_key: list_key.to_string(),
        }
    }

    /// Parse a `LLen` instance from an array frame.
    /// The 'LLen' String is already consumed.
    /// Returns the `LLen` instance on success or error if frame is malformed.
    ///
    /// Expects an array containing 2 entries.
    /// LLEN list_key
    pub(crate) fn parse_frames(parse: &mut Parse) -> Result<LLen, crate::Error> {
        let list_key = parse.next_string()?;
        Ok(LLen { list_key })
    }

    /// Execute the `LLen` command, the length of the list is sent to the client by writing the
    /// response to the `conn`.
    /// Returns the length of the list if successful or `WRONGTYPE` error if data item with
    /// `list_key` is not a list.
    /// Returns `0` if no list with `list_key` is found.
    pub(crate) async fn execute(&self, db: &Db, conn: &mut Connection) -> Result<(), crate::Error> {
        let maybe_list = db.get(&self.list_key);

        if let Some(list) = maybe_list {
            match list {
                Data::Array(list) => {
                    let response = Frame::Integer(list.len() as i64);
                    conn.write_frame(&response).await?;
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
            conn.write_frame(&Frame::Integer(0)).await?;
        }

        Ok(())
    }

    /// Convert `LLen` instance to `Frame`.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_string("llen".into());
        frame.push_string(self.list_key);

        frame
    }
}
