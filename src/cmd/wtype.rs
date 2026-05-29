use bytes::Bytes;

use crate::{
    Connection,
    db::{Data, Db},
    errors::WalrusError,
    frame::Frame,
    parse::Parse,
};

pub struct Type {
    key: String,
}

impl Type {
    /// Create a new `Type` command.
    pub fn new(key: impl ToString) -> Self {
        Type {
            key: key.to_string(),
        }
    }

    /// Parse the `Type` command from a frame iterator.
    /// The `type` string is already consumed.
    ///
    /// The frame must have 2 elements.
    /// TYPE key
    pub(crate) fn parse_frames(parse: &mut Parse) -> Result<Self, WalrusError> {
        let key = parse.next_string()?;
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
        let string_frame = Frame::Bulk(Bytes::from("string"));
        let none_frame = Frame::Bulk(Bytes::from("none"));
        let list_frame = Frame::Bulk(Bytes::from("list"));

        let maybe_data = db.get(self.key.as_str());
        if let Some(data) = maybe_data {
            match data {
                Data::Bytes(_) => conn.write_frame(&string_frame).await?,
                Data::Integer(_) => conn.write_frame(&string_frame).await?,
                Data::Double(_) => conn.write_frame(&string_frame).await?,
                Data::String(_) => conn.write_frame(&string_frame).await?,
                Data::Array(_) => conn.write_frame(&list_frame).await?,
            }
        } else {
            conn.write_frame(&none_frame).await?;
        }

        Ok(())
    }

    /// Convert `Type` instance to `Frame`.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_string("type".into());
        frame.push_string(self.key);

        frame
    }
}
