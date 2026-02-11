use crate::frame::Frame;
use atoi::atoi;
use bytes::Bytes;
use std::{fmt, vec};

/// For parsing a command.
///
/// Command are sent as Frame::Array(Frame). Provides parsing value from
/// each array frame one at a time.
pub(crate) struct Parse {
    /// Iterator over array frames.
    frames: vec::IntoIter<Frame>,
}

/// Parse errors. Only EndOfStream can be handled at runtime, all other errors should
/// terminate the connection.
#[derive(Debug)]
pub(crate) enum ParseError {
    /// Frame fully consumed, no more values can be extracted.
    EndOfStream,
    /// All other errors
    Other(crate::Error),
}

impl Parse {
    /// Creates a new `Parse` to parse the contents of frames.
    ///
    /// Returns `ParseError` if frame is not an array of frames.
    pub(crate) fn new(frame: Frame) -> Result<Parse, ParseError> {
        let array_of_frames = match frame {
            Frame::Array(array) => array,
            frame => return Err(format!("protocol error; expected array, got {frame:?}").into()),
        };

        Ok(Parse {
            frames: array_of_frames.into_iter(),
        })
    }

    /// Get the next frame in the array, consumes the frame.
    fn next(&mut self) -> Result<Frame, ParseError> {
        self.frames.next().ok_or(ParseError::EndOfStream)
    }

    /// Return the next array entry as raw bytes.
    ///
    /// error is returned if entry is not representable as raw bytes.
    pub(crate) fn next_bytes(&mut self) -> Result<Bytes, ParseError> {
        match self.next()? {
            // Simple and Bulk frames can be representated raw bytes,
            // errors are considered separate types despite them stored
            // as strings.
            Frame::Simple(data) => Ok(Bytes::from(data.into_bytes())),
            Frame::Bulk(data) => Ok(data),
            frame => Err(format!(
                "protocol error; expected simple or bulk string frame, got {frame:?}"
            )
            .into()),
        }
    }

    /// Returns next array entry as String.
    ///
    /// error is returned if next entry can't be represented as String.
    pub(crate) fn next_string(&mut self) -> Result<String, ParseError> {
        match self.next()? {
            // Both Simple and Bulk frames can be parsed to UTF-8.
            Frame::Simple(data) => Ok(data),
            Frame::Bulk(data) => match str::from_utf8(&data[..]) {
                Ok(str) => Ok(str.to_string()),
                Err(_) => Err(format!("protocol error; invalid string").into()),
            },
            frame => {
                Err(format!("protocol error; expected Simple or Bulk frame, got {frame:?}").into())
            }
        }
    }

    /// Returns next array entry as u64.
    ///
    /// error is returned if next entry can't be represented as u64.
    pub(crate) fn next_int(&mut self) -> Result<u64, ParseError> {
        match self.next()? {
            // Simple and Bulk can be parse to u64, error is returned if parsing fails.
            Frame::Simple(data) => {
                atoi::<u64>(data.as_bytes()).ok_or_else(|| "protocol error; invalid number".into())
            }
            Frame::Bulk(data) => {
                atoi::<u64>(&data).ok_or_else(|| "protocol error; invalid number".into())
            }
            Frame::Integer(int) => Ok(int),
            frame => Err(format!("protocol error; expected Integer frame, got {frame:?}").into()),
        }
    }
}

impl From<String> for ParseError {
    fn from(src: String) -> ParseError {
        ParseError::Other(src.into())
    }
}

impl From<&str> for ParseError {
    fn from(src: &str) -> ParseError {
        src.to_string().into()
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::EndOfStream => "protocol error; unexpected end of stream".fmt(f),
            ParseError::Other(err) => err.fmt(f),
        }
    }
}

impl std::error::Error for ParseError {}
