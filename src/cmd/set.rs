use bytes::Bytes;

use crate::{
    Connection,
    db::{self, Data, Db},
    errors::WalrusError,
    frame::Frame,
    parse::{Parse, ParseError},
};
use std::time::Duration;

/// Set a value for a key.
///
/// If key is already present it's value is overwritten.
pub struct Set {
    key: Bytes,
    value: Bytes,
    expire: Option<Duration>,
}

impl Set {
    /// Creates a new `Set` command which sets `key` to `value`
    /// If `expire` is provided then key will expire after specified duration.
    pub fn new(key: Bytes, value: Bytes, expire: Option<Duration>) -> Set {
        Set { key, value, expire }
    }

    /// Parse a `Set` instance from a received array frame.
    ///
    /// The `SET` string is already consumed.
    ///
    /// Returns the `Set` value on success. Error is returned if frame is malformed.
    /// Expects an array frame containing atleast 3 entries.
    ///
    /// SET key value [EX seconds|PX milliseconds]
    pub(crate) fn parse_frames(parse: &mut Parse) -> Result<Set, WalrusError> {
        // Get key from the frame.
        let key = parse.next_bytes()?;
        // Get the value to set from the frame.
        let value = parse.next_bytes()?;
        // Optional field.
        let mut expire = None;

        match parse.next_bytes() {
            Ok(s) if s.eq_ignore_ascii_case(b"ex") => {
                // Expiration in seconds, next value must be an integer.
                let secs = parse.next_int()?;
                expire = Some(Duration::from_secs(secs as u64));
            }
            Ok(s) if s.eq_ignore_ascii_case(b"px") => {
                // Expiration in milliseconds, next value must be an integer.
                let ms = parse.next_int()?;
                expire = Some(Duration::from_millis(ms as u64));
            }
            Ok(_) => return Err("walrus only supports expiration option for `SET`".into()),
            // No options specified for `SET`, no expiration is set.
            Err(ParseError::EndOfStream) => {}
            Err(err) => return Err(err.into()),
        }

        Ok(Set { key, value, expire })
    }

    /// Execute the `Set` command, inserting the given key-value pair into `Db`.
    /// "OK" response is written to `conn`.
    pub(crate) async fn execute(self, db: &Db, conn: &mut Connection) -> Result<(), WalrusError> {
        // optimize storage of data before inserting into db.
        let value = db::optimize_storage(self.value);

        db.set(&self.key, value, self.expire);

        let response = Data::Bytes(Bytes::from("OK"));
        conn.write_data(&response);

        Ok(())
    }

    /// Converts `Set` instance to `Frame`, consumes self.
    pub fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("set"));
        frame.push_bulk(self.key);
        frame.push_bulk(self.value);

        if let Some(ms) = self.expire {
            // Expiration can be specified in two ways
            // 1. SET key value EX seconds
            // 2. SET key value PX milliseconds
            // The later is used here for greater precision.
            frame.push_bulk(Bytes::from("px"));
            frame.push_int(ms.as_millis() as i64);
        }

        frame
    }
}
