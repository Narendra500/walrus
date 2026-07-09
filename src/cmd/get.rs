use bytes::Bytes;

use crate::{
    Connection,
    db::{Data, Db, double_to_bytes, int_to_bytes},
    errors::WalrusError,
    frame::Frame,
    parse::Parse,
};

/// Get the value of the key.
pub struct Get {
    key: Bytes,
}

impl Get {
    /// Create a new `Get` instance which fetches `key`
    pub fn new(key: Bytes) -> Get {
        Get { key }
    }

    /// Parse a `Get` instance from array frame.
    /// The `GET` string is already consumed.
    ///
    /// Returns `Get` instance on success, if the frame is malformed an error is returned.
    ///
    /// Expects an array frame containing exactly two entries.
    /// GET key
    pub(crate) fn parse_frame(parse: &mut Parse) -> Result<Get, WalrusError> {
        let key = parse.next_bytes()?;
        Ok(Get { key })
    }

    /// Execute the `Get` command to fetch the value for the key from the shared db.
    /// The value is written to `conn`.
    pub(crate) async fn execute(self, db: &Db, conn: &mut Connection) -> Result<(), WalrusError> {
        let maybe_data = db.get(&self.key);

        match maybe_data {
            Some(data) => match data {
                Data::Array(_) => {
                    conn.write_error_frame(WalrusError::WrongType.get_msg());
                    return Err(WalrusError::WrongType);
                }
                Data::Bytes(bytes) => conn.write_data(&Data::Bytes(bytes)),
                Data::Integer(integer) => conn.write_data(&Data::Bytes(int_to_bytes(integer))),
                Data::Double(double) => conn.write_data(&Data::Bytes(double_to_bytes(double))),
                Data::String(string) => conn.write_data(&Data::String(string)),
            },
            None => conn.write_null_frame(),
        };

        Ok(())
    }

    /// Convert `Get` instance to `Frame`.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("get"));
        frame.push_bulk(self.key);
        frame
    }
}
