use crate::{
    db::{Data, optimize_storage},
    errors::WalrusError,
    frame::Frame,
};
use bytes::Bytes;
use std::{collections::VecDeque, fmt, iter::Peekable, vec};

/// For parsing a command.
///
/// Command are sent as Frame::Array(Frame). Provides parsing value from
/// each array frame one at a time.
#[derive(Debug)]
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
    /// Returns (Vec<Bytes>, f64) on success.
    pub(crate) fn next_bytes_with_timeout(&mut self) -> Result<(Vec<Bytes>, f64), ParseError> {
        let mut result = Vec::new();
        while let Ok(frame) = self.next() {
            match frame {
                Frame::Simple(data) => {
                    // If this is the last element of the blpop command then must be the timeout.
                    if self.peek().is_err() {
                        // parse int or float from bytes.
                        let timeout = optimize_storage(Bytes::from(data));
                        match timeout {
                            Data::Double(t) => return Ok((result, t)),
                            Data::Integer(t) => return Ok((result, t as f64)),
                            _ => return Err("protocol error; last item must be the timeout".into()),
                        }
                    }
                    result.push(Bytes::from(data));
                }
                Frame::Integer(data) => {
                    if result.is_empty() {
                        return Err("protocol error; No keys specified in BLPOP".into());
                    }
                    // The timeout is the last element, so any more data is invalid.
                    let timeout = data as f64;
                    if self.peek().is_ok() {
                        return Err(
                            "protocol error; data item after timeout not allowed in BLPOP".into(),
                        );
                    };

                    return Ok((result, timeout));
                }
                Frame::Double(data) => {
                    if result.is_empty() {
                        return Err("protocol error; No keys specified in BLPOP".into());
                    }
                    // The timeout is the last element, so any more data is invalid.
                    let timeout = data;
                    if self.peek().is_ok() {
                        return Err(
                            "protocol error; data item after timeout not allowed in BLPOP".into(),
                        );
                    };

                    return Ok((result, timeout));
                }
                Frame::Bulk(bytes) => {
                    // If this is the last element of the blpop command then must be the timeout.
                    if self.peek().is_err() {
                        // parse int or float from bytes.
                        let timeout = optimize_storage(bytes);
                        match timeout {
                            Data::Double(t) => return Ok((result, t)),
                            Data::Integer(t) => return Ok((result, t as f64)),
                            _ => return Err("protocol error; last item must be the timeout".into()),
                        }
                    }
                    result.push(bytes);
                }
                Frame::Error(err) => {
                    return Err(ParseError::Other(err.into()));
                }
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
                Frame::Double(data) => result.push_back(Data::Double(data)),
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
            Frame::Bulk(data) => Ok(data),
            Frame::Simple(data) => Ok(Bytes::from(data)),
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
                extract_i64(data.as_bytes()).ok_or_else(|| "protocol error; invalid number".into())
            }
            Frame::Bulk(data) => {
                extract_i64(&data).ok_or_else(|| "protocol error; invalid number".into())
            }
            Frame::Integer(int) => Ok(int),
            frame => Err(format!("protocol error; expected Integer frame, got {frame:?}").into()),
        }
    }
}

pub(crate) fn extract_f64(bytes: &[u8]) -> Option<f64> {
    use fast_float;
    if !check_float_trailing_zeros(bytes) {
        return fast_float::parse::<f64, _>(bytes.as_ref()).ok();
    }
    None
}

/// Checks if the float slice has trailing zeros.
/// Special case for floats with just a single trailing zero right after the decimal point, parsing
/// them doesn't change the value and hence false is returned.
fn check_float_trailing_zeros(bytes: &[u8]) -> bool {
    let mut number_of_trailing_zeros = 0;
    let mut rev_iter = bytes.iter().rev();

    while let Some(&byte) = rev_iter.next() {
        if byte == b'0' {
            number_of_trailing_zeros += 1;
        } else if byte == b'.' {
            return number_of_trailing_zeros > 1;
        } else {
            return false;
        }
    }

    // Only zeroes, no decimal point or any other number was found.
    number_of_trailing_zeros > 1
}

/// Extracts i64 from bytes.
/// #Rejects
/// - integers with leading zeroes to avoid changing values like '001' to 1.
/// - integers starting with a + sign.
/// - byte slice containing non digit characters.
///
/// Returns i64 if successful else None.
pub(crate) fn extract_i64_strict(bytes: &[u8]) -> Option<i64> {
    if bytes.is_empty() {
        return None;
    }

    let mut result: i64 = 0;
    let mut is_negative = false;
    let mut start_idx = 0;

    // Check if the first byte is a minus sign.
    if bytes[0] == b'-' {
        is_negative = true;
        start_idx = 1;
        // If it's just a "-" with no numbers, return None.
        if bytes.len() == 1 {
            return None;
        }
    }
    // If + is included then don't parse the number. As +91 denotes country code and should not be
    // parse as integer.
    else if bytes[0] == b'+' {
        return None;
    }

    for &byte in &bytes[start_idx..] {
        // Ensure that the byte is an ASCII digit.
        if byte >= b'0' && byte <= b'9' {
            let digit = (byte - b'0') as i64;

            // If leading zeroes are present then turning into integer will change the actual value.
            // '001' will be parse as 1. Which is not intended.
            if result == 0 && digit == 0 {
                return None;
            }

            // Multiply the current result by 10 and add the new digit.
            // Using checked_mul and checked_add to avoid overflow.
            result = result.checked_mul(10)?.checked_add(digit)?;
        } else {
            // If the byte is not a digit, return None.
            return None;
        }
    }

    if is_negative {
        Some(-result)
    } else {
        Some(result)
    }
}

/// Extracts i64 from bytes.
/// #Rejects
/// - byte slice containing non digit characters.
///
/// Returns i64 if successful else None.
pub(crate) fn extract_i64(bytes: &[u8]) -> Option<i64> {
    if bytes.is_empty() {
        return None;
    }

    let mut result: i64 = 0;
    let mut is_negative = false;
    let mut start_idx = 0;

    // Check if the first byte is a minus sign.
    if bytes[0] == b'-' {
        is_negative = true;
        start_idx = 1;
        // If it's just a "-" with no numbers, return None.
        if bytes.len() == 1 {
            return None;
        }
    }

    for &byte in &bytes[start_idx..] {
        // Ensure that the byte is an ASCII digit.
        if byte >= b'0' && byte <= b'9' {
            let digit = (byte - b'0') as i64;

            // Multiply the current result by 10 and add the new digit.
            // Using checked_mul and checked_add to avoid overflow.
            result = result.checked_mul(10)?.checked_add(digit)?;
        } else {
            // If the byte is not a digit, return None.
            return None;
        }
    }

    if is_negative {
        Some(-result)
    } else {
        Some(result)
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
