//! Provides a type represting a RESP frame as well as utilities for
//! parsing frames from a byte array.

use bytes::{Buf, Bytes};
use core::fmt;
use std::string::FromUtf8Error;
use std::{io::Cursor, num::TryFromIntError};

/// A frame in RESP.
///
/// Size of each frame instance:
///
/// Integer(u64): Requires 8 bytes.
/// Simple(String): Requires 24 bytes (pointer + len + cap).
/// Bulk(Bytes): Bytes is usually 32 bytes.
/// Array(Vec<Frame>): A Vec Requires 24 bytes (pointer + len + cap).
///
/// The largest variant is Bulk(Bytes) at ~32 bytes. It adds 1 byte for the enum tag. padding to align memory correctly.
///
/// So, every single Frame instance will take up roughly 40 bytes in memory (32 + 1 + padding).
#[derive(Debug, PartialEq)]
pub enum Frame {
    Simple(String),
    Error(String),
    Integer(u64),
    Bulk(Bytes),
    Null,
    Array(Vec<Frame>),
}

/// Error::Incomplete; Not enough data is available to parse a message
/// Error::Other; Invalid message encoding
#[derive(Debug)]
pub enum Error {
    Incomplete,
    Other(crate::Error),
}

impl Frame {
    /// Push Frame::Simple(String) into an array frame.
    /// Will `panic` if called by non array frame.
    pub(crate) fn push_string(&mut self, string: String) {
        match self {
            Frame::Array(frame) => frame.push(Frame::Simple(string)),
            _ => panic!("not an array frame"),
        }
    }

    /// Push Frame::Bulk(Bytes) into an array frame.
    /// Will `panic` if called by non array frame.
    pub(crate) fn push_bulk(&mut self, bytes: Bytes) {
        match self {
            Frame::Array(frame) => frame.push(Frame::Bulk(bytes)),
            _ => panic!("not an array frame"),
        }
    }

    /// Push Frame::Integer(u64) into an array frame.
    /// Will `panic` if called by non array frame.
    pub(crate) fn push_int(&mut self, val: u64) {
        match self {
            Frame::Array(frame) => frame.push(Frame::Integer(val)),
            _ => panic!("not an array frame"),
        }
    }

    /// Check if entire message can be decoded from 'src'
    pub fn check(src: &mut Cursor<&[u8]>) -> Result<(), Error> {
        match get_u8(src)? {
            b'+' => {
                get_line(src)?;
                Ok(())
            }
            b'-' => {
                get_line(src)?;
                Ok(())
            }
            b':' => {
                get_decimal(src)?;
                Ok(())
            }
            b'$' => {
                if b'-' == peek_u8(src)? {
                    // skip -1\r\n
                    skip(src, 4)
                } else {
                    // Read the bulk string
                    // `try_into` fails if the number doesn't fit in usize, for example on 32 bit
                    // computer u64 may not fit in usize (32 bit)
                    let len: usize = get_decimal(src)?.try_into()?;

                    // skip `len` number of bytes + 2 (\r\n)
                    skip(src, len + 2)
                }
            }
            b'*' => {
                if b'-' == peek_u8(src)? {
                    // skip -1\r\n
                    skip(src, 4)
                } else {
                    let len: usize = get_decimal(src)?.try_into()?;

                    for _ in 0..len {
                        Frame::check(src)?;
                    }

                    Ok(())
                }
            }
            actual => Err(format!("protocol error; invalid frame type byte `{}`", actual).into()),
        }
    }

    /// Parse message from `src`
    pub fn parse(src: &mut Cursor<&[u8]>) -> Result<Frame, Error> {
        match get_u8(src)? {
            b'+' => {
                let line = get_line(src)?.to_vec();

                let string = String::from_utf8(line)?;

                Ok(Frame::Simple(string))
            }
            b'-' => {
                let line = get_line(src)?.to_vec();

                let string = String::from_utf8(line)?;

                Ok(Frame::Error(string))
            }
            b':' => {
                let number = get_decimal(src)?;
                Ok(Frame::Integer(number))
            }
            b'$' => {
                // $-1\r\n is Null
                if b'-' == peek_u8(src)? {
                    let line = get_line(src)?;

                    if line != b"-1" {
                        return Err("protocol error; invalid frame format".into());
                    }

                    Ok(Frame::Null)
                } else {
                    // Read the bulk string
                    // `try_into` fails if the number doesn't fit in usize, for example on 32 bit
                    // computer u64 may not fit in usize (32 bit)
                    let len: usize = get_decimal(src)?.try_into()?;
                    let len_inclusive_crlf = len + 2;

                    if src.remaining() < len_inclusive_crlf {
                        return Err(Error::Incomplete);
                    }

                    let data = Bytes::copy_from_slice(&src.chunk()[..len]);

                    // skip `len_inclusive_crlf` number of bytes
                    skip(src, len_inclusive_crlf)?;

                    Ok(Frame::Bulk(data))
                }
            }
            b'*' => {
                if b'-' == peek_u8(src)? {
                    let line = get_line(src)?;

                    if line != b"-1" {
                        return Err("protocol error; invalid frame format".into());
                    }

                    Ok(Frame::Null)
                } else {
                    let len: usize = get_decimal(src)?.try_into()?;
                    let mut out_vec = Vec::with_capacity(len);

                    for _ in 0..len {
                        out_vec.push(Frame::parse(src)?);
                    }

                    Ok(Frame::Array(out_vec))
                }
            }
            _ => unimplemented!(),
        }
    }

    /// Returns an empty array
    pub(crate) fn array() -> Frame {
        Frame::Array(vec![])
    }
}

/// Get byte at current cursor position without advancing the cursor.
fn peek_u8(src: &mut Cursor<&[u8]>) -> Result<u8, Error> {
    if !src.has_remaining() {
        return Err(Error::Incomplete);
    }

    Ok(src.chunk()[0])
}

/// Get byte at current cursor position; advances the cursor position by one.
fn get_u8(src: &mut Cursor<&[u8]>) -> Result<u8, Error> {
    if !src.has_remaining() {
        return Err(Error::Incomplete);
    }

    Ok(src.get_u8())
}

/// Skip `n` bytes advancing the cursor position by `n`.
fn skip(src: &mut Cursor<&[u8]>, n: usize) -> Result<(), Error> {
    if !src.has_remaining() {
        return Err(Error::Incomplete);
    }

    src.advance(n);
    Ok(())
}

/// Read a CRLF terminated decimal.
fn get_decimal(src: &mut Cursor<&[u8]>) -> Result<u64, Error> {
    use atoi::atoi;

    let line = get_line(src)?;

    atoi::<u64>(line).ok_or_else(|| "protocol error; invalid frame format".into())
}

/// Get all bytes until next CRLF.
fn get_line<'a>(src: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], Error> {
    let start = src.position() as usize;

    // Get the slice of bytes starting from current cursor position upto buffer len.
    let remaining = &src.get_ref()[start..];

    // windows(2) returns an iterator to iterate 2 bytes at a time.
    // .position() returns the index where the closure `|window| window == b"\r\n"` returns true.
    if let Some(offset) = remaining.windows(2).position(|window| window == b"\r\n") {
        src.set_position((start + offset + 2) as u64);

        return Ok(&remaining[..offset]);
    }

    Err(Error::Incomplete)
}

impl fmt::Display for Frame {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Frame::Simple(string) => string.fmt(fmt),
            Frame::Error(err) => write!(fmt, "error: {err}"),
            Frame::Integer(num) => num.fmt(fmt),
            Frame::Bulk(msg) => match str::from_utf8(&msg) {
                // valid text
                Ok(string) => string.fmt(fmt),
                // print raw bytes
                Err(_) => write!(fmt, "{msg:?}"),
            },
            Frame::Null => "(nil)".fmt(fmt),
            Frame::Array(frame_vec) => {
                // Add "[" signaling start of an array
                write!(fmt, "[")?;

                let mut iter = frame_vec.iter();

                // fmt is called for each vector element, taking care of case where element is an
                // array
                // print first element of the array
                if let Some(first) = iter.next() {
                    first.fmt(fmt)?;
                }

                // print remaning parts space separated
                for part in iter {
                    write!(fmt, " {}", part)?;
                }

                // Add "]" signaling end of an array
                write!(fmt, "]")?;

                Ok(())
            }
        }
    }
}

impl From<FromUtf8Error> for Error {
    fn from(_src: FromUtf8Error) -> Error {
        "protocol error; invalid framte format".into()
    }
}

impl From<String> for Error {
    fn from(src: String) -> Error {
        Error::Other(src.into())
    }
}

impl From<&str> for Error {
    fn from(src: &str) -> Error {
        src.to_string().into()
    }
}

impl From<TryFromIntError> for Error {
    fn from(_src: TryFromIntError) -> Error {
        "protocol error; invalid frame format.".into()
    }
}

impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Incomplete => "stream ended early".fmt(fmt),
            Error::Other(err) => err.fmt(fmt),
        }
    }
}
