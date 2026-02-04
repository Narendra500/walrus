use crate::connection::Connection;
use crate::frame::Frame;
use crate::parse::{Parse, ParseError};
use bytes::Bytes;

/// PING command, returns PONG if no message provided,
/// else repeats the message back to sender.
///
/// Used to check if connection is still alive.
#[derive(Debug)]
pub struct Ping {
    /// Optional message to be returned.
    msg: Option<Bytes>,
}

impl Ping {
    /// Creates a new `PING` command with optional `msg`.
    pub fn new(msg: Option<Bytes>) -> Ping {
        Ping { msg }
    }

    /// Parse a `Ping` instance.
    /// The 'PING' string is already consumed.
    /// Returns `Ping` value on success. error is returned if frame is malformed.
    /// Expects parse instance containing the array frame of 'PING' and
    /// optional message.
    pub(crate) fn parse_frames(parse: &mut Parse) -> Result<Ping, crate::Error> {
        // Try to parse message if any.
        match parse.next_bytes() {
            Ok(msg) => Ok(Ping { msg: Some(msg) }),
            Err(ParseError::EndOfStream) => Ok(Ping { msg: None }),
            Err(e) => Err(e.into()),
        }
    }

    /// Send back `Ping` message to the client.
    pub(crate) async fn execute(self, conn: &mut Connection) -> Result<(), crate::Error> {
        let response = match self.msg {
            None => Frame::Simple(String::from("PONG")),
            Some(msg) => Frame::Bulk(msg),
        };

        // Send message to client.
        conn.write_frame(&response).await?;

        Ok(())
    }
}
