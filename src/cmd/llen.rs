use bytes::Bytes;

use crate::{
    Connection,
    db::{Data, Db},
    errors::WalrusError,
    frame::Frame,
    parse::Parse,
};

/// `LLen` command to get the length of a list.
pub struct LLen {
    list_key: Bytes,
}

impl LLen {
    /// Returns a `LLen` instance.
    /// Takes key which can be of any datatype that implements `ToString`.
    pub fn new(list_key: Bytes) -> LLen {
        LLen { list_key }
    }

    /// Parse a `LLen` instance from an array frame.
    /// The 'LLen' String is already consumed.
    /// Returns the `LLen` instance on success or error if frame is malformed.
    ///
    /// Expects an array containing 2 entries.
    /// LLEN list_key
    pub(crate) fn parse_frames(parse: &mut Parse) -> Result<LLen, WalrusError> {
        let list_key = parse.next_bytes()?;
        Ok(LLen { list_key })
    }

    /// Execute the `LLen` command, the length of the list is sent to the client by writing the
    /// response to the `conn`.
    /// Returns the length of the list if successful or `WRONGTYPE` error if data item with
    /// `list_key` is not a list.
    /// Returns `0` if no list with `list_key` is found.
    pub(crate) async fn execute(&self, db: &Db, conn: &mut Connection) -> Result<(), WalrusError> {
        let maybe_list = db.get(&self.list_key);

        if let Some(list) = maybe_list {
            match list {
                Data::Array(list) => {
                    let response = Data::Integer(list.len() as i64);
                    conn.write_data(&response);
                }
                // Data associated with the given key is not a list.
                _ => {
                    conn.write_error_frame(WalrusError::WrongType.get_msg());
                }
            }
        }
        // No list with given key.
        else {
            conn.write_data(&Data::Integer(0));
        }

        Ok(())
    }

    /// Convert `LLen` instance to `Frame`.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("llen"));
        frame.push_bulk(self.list_key);

        frame
    }
}
