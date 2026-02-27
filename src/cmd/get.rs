use crate::{
    Connection,
    db::{Data, Db},
    frame::Frame,
    parse::Parse,
};

/// Get the value of the key.
pub struct Get {
    key: String,
}

impl Get {
    /// Create a new `Get` instance which fetches `key`
    pub fn new(key: impl ToString) -> Get {
        Get {
            key: key.to_string(),
        }
    }

    /// Parse a `Get` instance from array frame.
    /// The `GET` string is already consumed.
    ///
    /// Returns `Get` instance on success, if the frame is malformed an error is returned.
    ///
    /// Expects an array frame containing exactly two entries.
    /// GET key
    pub(crate) fn parse_frame(parse: &mut Parse) -> Result<Get, crate::Error> {
        let key = parse.next_string()?;
        Ok(Get { key })
    }

    /// Execute the `Get` command to fetch the value for the key from the shared db.
    /// The value is written to `conn`.
    pub(crate) async fn execute(self, db: &Db, conn: &mut Connection) -> Result<(), crate::Error> {
        let maybe_data = db.get(&self.key);

        let frame = match maybe_data {
            Some(data) => match data {
                Data::Bytes(b) => Frame::Bulk(b),
                Data::String(s) => Frame::Bulk(s.into()),
                Data::Integer(i) => Frame::Integer(i),
                Data::Array(_) => {
                    return Err(
                        "ERR Operation against a key holding the wrong kind of value".into(),
                    );
                }
            },
            None => Frame::Null,
        };

        conn.write_frame(&frame).await.unwrap();
        Ok(())
    }

    /// Convert `Get` instance to `Frame`.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_string("get".to_string());
        frame.push_string(self.key);
        frame
    }
}
