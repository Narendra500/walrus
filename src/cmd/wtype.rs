use bytes::Bytes;

use crate::{
    Connection,
    db::{Data, Db},
    errors::WalrusError,
    frame::Frame,
    parse::Parse,
};

pub struct Type {
    key: Bytes,
}

impl Type {
    /// Create a new `Type` command.
    pub fn new(key: Bytes) -> Self {
        Type { key }
    }

    /// Parse the `Type` command from a frame iterator.
    /// The `type` string is already consumed.
    ///
    /// The frame must have 2 elements.
    /// TYPE key
    pub(crate) fn parse_frames(parse: &mut Parse) -> Result<Self, WalrusError> {
        let key = parse.next_bytes()?;
        Ok(Type::new(key))
    }

    /// Execute the `Type` command.
    ///
    /// Writes the type of the data associated with the given key to the client
    /// connection if successful.
    ///
    /// Writes "none" if the key doesn't exist.
    /// Writes "list" if the data associated with the key is a list.
    /// Writes "string" for Bytes, Integer, Double and String.
    /// Although Integer and Double are stored as i64 and f64 internally, the type
    /// presented to the client is string.
    pub(crate) async fn execute(&self, db: &Db, conn: &mut Connection) -> Result<(), WalrusError> {
        let string = Data::Bytes(Bytes::from("string"));
        let none = Data::Bytes(Bytes::from("none"));
        let list = Data::Bytes(Bytes::from("list"));

        let maybe_data = db.get(&self.key);
        if let Some(data) = maybe_data {
            match data {
                Data::Bytes(_) => conn.write_data(&string),
                Data::Integer(_) => conn.write_data(&string),
                Data::Double(_) => conn.write_data(&string),
                Data::String(_) => conn.write_data(&string),
                Data::Array(_) => conn.write_data(&list),
            }
        } else {
            conn.write_data(&none);
        }

        Ok(())
    }

    /// Convert `Type` instance to `Frame`.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("type"));
        frame.push_bulk(self.key);

        frame
    }
}
