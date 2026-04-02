use crate::{db::Data, errors::WalrusError, frame::Frame};
use atoi::atoi;
use bytes::Bytes;
use std::{collections::VecDeque, fmt, iter::Peekable, vec};

/// For parsing a command.
///
/// Command are sent as Frame::Array(Frame). Provides parsing value from
/// each array frame one at a time.
pub(crate) struct Parse {
    /// Iterator over array frames.
    frames: Peekable<vec::IntoIter<Frame>>,
}

/// Parse errors. Only EndOfStream can be handled at runtime, all other errors should
/// terminate the connection.
#[derive(Debug)]
pub(crate) enum ParseError {
    /// Frame fully consumed, no more values can be extracted.
    EndOfStream,
    /// Client closed the connection before parsing was completed.
    ConnectionClosed,
    /// All other errors
    Other(WalrusError),
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
            frames: array_of_frames.into_iter().peekable(),
        })
    }

    /// Get the next frame in the array, consumes the frame.
    fn next(&mut self) -> Result<Frame, ParseError> {
        self.frames.next().ok_or(ParseError::EndOfStream)
    }

    /// Peek the next frame in the array, does not consume the frame.
    fn peek(&mut self) -> Result<Frame, ParseError> {
        self.frames.peek().cloned().ok_or(ParseError::EndOfStream)
    }

    /// Try to parse any number of strings and a timeout.
    /// Returns (Vec<String>, u64) on success.
    pub(crate) fn next_strings_with_timeout(&mut self) -> Result<(Vec<String>, u64), ParseError> {
        let mut result = Vec::new();
        while let Ok(frame) = self.next() {
            match frame {
                Frame::Simple(data) => result.push(data),
                Frame::Integer(data) => {
                    if result.is_empty() {
                        return Err("protocol error; No keys specified in BLPOP".into());
                    }
                    // The timeout is the last element, so any more data is invalid.
                    let timeout = data as u64;
                    if self.peek().is_ok() {
                        return Err(
                            "protocol error; data item after timeout not allowed in BLPOP".into(),
                        );
                    };

                    return Ok((result, timeout));
                }
                Frame::Bulk(bytes) => {
                    let maybe_string = String::from_utf8(bytes.to_vec());
                    match maybe_string {
                        Ok(string) => result.push(string),
                        Err(_) => {
                            return Err("protocol error; invalid string in BLPOP".into());
                        }
                    };
                }
                Frame::Error(err) => return Err(ParseError::Other(err.into())),
                Frame::Null => {
                    return Err("protocol error; null not allowed in BLPOP".into());
                }
                Frame::Array(_) => {
                    return Err("protocol error; array not allowed in BLPOP".into());
                }
            }
        }

        // Connection closed before parsing was completed.
        Err(ParseError::ConnectionClosed)
    }

    /// Try to parse all the elements left in the array.
    /// Returns VecDeque of Data.
    pub(crate) fn next_array(&mut self) -> Result<VecDeque<Data>, ParseError> {
        let mut result = VecDeque::new();
        while let Ok(frame) = self.next() {
            match frame {
                Frame::Simple(data) => result.push_back(Data::String(data)),
                Frame::Bulk(data) => result.push_back(Data::Bytes(data)),
                Frame::Integer(data) => result.push_back(Data::Integer(data)),
                Frame::Error(err) => return Err(ParseError::Other(err.into())),
                Frame::Null => {
                    return Err(ParseError::Other("can't push null values in array".into()));
                }
                Frame::Array(_) => {
                    result.push_back(Data::Array(self.next_array()?));
                }
            }
        }

        Ok(result)
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

    /// Returns next array entry as i64.
    ///
    /// error is returned if next entry can't be represented as u64.
    pub(crate) fn next_int(&mut self) -> Result<i64, ParseError> {
        match self.next()? {
            // Simple and Bulk can be parse to i64, error is returned if parsing fails.
            Frame::Simple(data) => {
                atoi::<i64>(data.as_bytes()).ok_or_else(|| "protocol error; invalid number".into())
            }
            Frame::Bulk(data) => {
                atoi::<i64>(&data).ok_or_else(|| "protocol error; invalid number".into())
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
            ParseError::ConnectionClosed => "connection abruptly closed".fmt(f),
        }
    }
}

impl std::error::Error for ParseError {}
