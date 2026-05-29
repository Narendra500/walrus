use crate::{
    Connection,
    db::{Data, Db, double_to_bytes, int_to_bytes},
    errors::WalrusError,
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
    pub(crate) fn parse_frame(parse: &mut Parse) -> Result<Get, WalrusError> {
        let key = parse.next_string()?;
        Ok(Get { key })
    }

    /// Execute the `Get` command to fetch the value for the key from the shared db.
    /// The value is written to `conn`.
    pub(crate) async fn execute(self, db: &Db, conn: &mut Connection) -> Result<(), WalrusError> {
        let maybe_data = db.get(&self.key);

        match maybe_data {
            Some(data) => match data {
                Data::Array(_) => {
                    conn.write_frame(&Frame::Error(WalrusError::WrongType.get_msg().into()))
                        .await?;
                    return Err(WalrusError::WrongType);
                }
                Data::Bytes(bytes) => conn.write_frame(&Frame::Bulk(bytes)).await?,
                Data::Integer(integer) => {
                    conn.write_frame(&Frame::Bulk(int_to_bytes(integer)))
                        .await?
                }
                Data::Double(double) => {
                    conn.write_frame(&Frame::Bulk(double_to_bytes(double)))
                        .await?
                }
                Data::String(string) => conn.write_frame(&Frame::Simple(string)).await?,
            },
            None => conn.write_frame(&Frame::Null).await?,
        };

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
