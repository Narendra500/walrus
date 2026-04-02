use crate::parse::ParseError;
use core::fmt;

const WRONGTYPE_ERR: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";
const INTERNAL_ERR: &str = "Internal error";
const CONNECTION_CLOSED_ERR: &str = "Connection closed";
const END_OF_STREAM_ERR: &str = "End of stream";

#[derive(Debug)]
pub(crate) enum WalrusError {
    WrongType,
    EndOfStream,
    Internal(String),
    ConnectionClosed,
}

impl WalrusError {
    pub(crate) fn get_msg(&self) -> &str {
        match self {
            WalrusError::WrongType => WRONGTYPE_ERR,
            WalrusError::EndOfStream => END_OF_STREAM_ERR,
            WalrusError::Internal(msg) => msg,
            WalrusError::ConnectionClosed => CONNECTION_CLOSED_ERR,
        }
    }
}

impl std::fmt::Display for WalrusError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WalrusError::WrongType => fmt::Display::fmt(WRONGTYPE_ERR, f),
            WalrusError::EndOfStream => fmt::Display::fmt(END_OF_STREAM_ERR, f),
            WalrusError::Internal(msg) => fmt::Display::fmt(msg, f),
            WalrusError::ConnectionClosed => fmt::Display::fmt(CONNECTION_CLOSED_ERR, f),
        }
    }
}

impl From<std::io::Error> for WalrusError {
    fn from(err: std::io::Error) -> Self {
        WalrusError::Internal(err.to_string())
    }
}

impl std::error::Error for WalrusError {}

impl From<ParseError> for WalrusError {
    fn from(err: ParseError) -> Self {
        match err {
            ParseError::ConnectionClosed => WalrusError::ConnectionClosed,
            // Not acutally an error, just signifies that all input has been consumed.
            ParseError::EndOfStream => WalrusError::EndOfStream,
            ParseError::Other(err) => WalrusError::Internal(err.to_string()),
        }
    }
}
