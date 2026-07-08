//! Provides a type represting a RESP frame as well as utilities for
//! parsing frames from a byte array.

use bytes::{Buf, Bytes};
use core::fmt;
use std::collections::VecDeque;
use std::string::FromUtf8Error;
use std::{io::Cursor, num::TryFromIntError};

use crate::db::Data;
use crate::errors::WalrusError;
use crate::parse;

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
#[derive(Debug, PartialEq, Clone)]
pub enum Frame {
    Simple(Bytes),
    Error(String),
    Integer(i64),
    Double(f64),
    Bulk(Bytes),
    Null,
    Array(Vec<Frame>),
}

/// Error::Incomplete; Not enough data is available to parse a message
/// Error::Other; Invalid message encoding
#[derive(Debug)]
pub enum Error {
    Incomplete,
    Other(WalrusError),
}

impl Frame {
    /// Push Frame into an array frame.
    /// self needs to be an array frame.
    /// Takes Simple, Bulk, Integer and Double frames.
    pub(crate) fn push(&mut self, frame: Frame) {
        match frame {
            Frame::Simple(string) => self.push_string(string),
            Frame::Bulk(bytes) => self.push_bulk(bytes),
            Frame::Integer(val) => self.push_int(val),
            Frame::Double(val) => self.push_double(val),
            // Nested array's are not supported.
            _ => unreachable!(),
        }
    }

    /// Push Frame::Simple(String) into an array frame.
    /// Will `panic` if called by non array frame.
    pub(crate) fn push_string(&mut self, string: Bytes) {
        match self {
            Frame::Array(frame) => frame.push(Frame::Simple(string)),
            _ => panic!("not an array frame"),
        }
    }

    /// Push `Data` into an array frame.
    /// uses recursion to push nested arrays of `Data`.
    /// Will `panic` if `data_vec` contains nested arrays.
    pub(crate) fn push_data(&mut self, data_vec: VecDeque<Data>) {
        for data in data_vec {
            match data {
                Data::String(data) => self.push_string(data),
                Data::Bytes(data) => self.push_bulk(data),
                Data::Integer(data) => self.push_int(data),
                Data::Double(data) => self.push_double(data),
                // Nested arrays are not supported.
                Data::Array(_) => {
                    panic!("Nested arrays are not supported.");
                }
            }
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

    pub(crate) fn push_double(&mut self, val: f64) {
        match self {
            Frame::Array(frame) => frame.push(Frame::Double(val)),
            _ => panic!("not an array frame"),
        }
    }

    /// Push Frame::Integer(u64) into an array frame.
    /// Will `panic` if called by non array frame.
    pub(crate) fn push_int(&mut self, val: i64) {
        match self {
            Frame::Array(frame) => frame.push(Frame::Integer(val)),
            _ => panic!("not an array frame"),
        }
    }

    /// Check if a complete frame exists in the buffer without consuming it.
    /// If a frame can be parsed then the length of the complete frame is returned in bytes.
    pub fn check(src: &mut Cursor<&[u8]>) -> Result<usize, Error> {
        let start = src.position() as usize;
        match get_u8(src)? {
            b'+' | b'-' => {
                get_line(src)?;
                Ok(src.position() as usize - start)
            }
            b':' => {
                get_decimal(src)?;
                Ok(src.position() as usize - start)
            }
            b',' => {
                get_double(src)?;
                Ok(src.position() as usize - start)
            }
            b'$' => {
                // $-1\r\n is Null
                if b'-' == peek_u8(src)? {
                    let line = get_line(src)?;

                    if line != b"-1" {
                        return Err("protocol error; invalid frame format".into());
                    }

                    Ok(src.position() as usize - start)
                } else {
                    // Read the bulk string
                    // `try_into` fails if the number doesn't fit in usize, for example on 32 bit
                    // computer u64 may not fit in usize (32 bit)
                    let len: usize = get_decimal(src)?.try_into()?;
                    let len_inclusive_crlf = len + 2;

                    if src.remaining() < len_inclusive_crlf {
                        return Err(Error::Incomplete);
                    }

                    // skip `len_inclusive_crlf` number of bytes
                    skip(src, len_inclusive_crlf)?;

                    Ok(src.position() as usize - start)
                }
            }
            b'*' => {
                if b'-' == peek_u8(src)? {
                    let line = get_line(src)?;

                    if line != b"-1" {
                        return Err("protocol error; invalid frame format".into());
                    }

                    Ok(src.position() as usize - start)
                } else {
                    let len: usize = get_decimal(src)?.try_into()?;

                    for _ in 0..len {
                        Frame::check(src)?;
                    }

                    Ok(src.position() as usize - start)
                }
            }
            b => {
                return Err(format!(
                    "protocol error; invalid frame format. Unexpected byte: {}",
                    b
                )
                .into());
            }
        }
    }

    /// Parse message from `src`.
    /// The frame contains just enough data to parse a frame, doesn't include the \r\n at the end of
    /// the frame.
    pub fn parse(src: &mut Bytes) -> Result<Frame, Error> {
        // get_u8 panics if no data is avaiable in the buffer, but its safe here as check phase
        // would have confirmed that enough data is available for a frame here.
        match src.get_u8() {
            b'+' => {
                let line = get_line_from_bytes(src)?;

                Ok(Frame::Simple(line))
            }
            b'-' => {
                let line = get_line_from_bytes(src)?;
                let err = String::from_utf8(line.to_vec())?;
                Ok(Frame::Error(err))
            }
            b':' => {
                let number = get_decimal_from_bytes(src)?;

                Ok(Frame::Integer(number))
            }
            b',' => {
                let number = get_double_from_bytes(src)?;
                Ok(Frame::Double(number))
            }
            b'$' => {
                // $-1\r\n is Null
                if b'-' == peek_u8(src)? {
                    let line = get_line_from_bytes(src)?;
                    if *line != *b"-1" {
                        return Err("protocol error; invalid frame format".into());
                    }

                    Ok(Frame::Null)
                } else {
                    // Read the bulk string
                    // `try_into` fails if the number doesn't fit in usize, for example on 32 bit
                    // computer u64 may not fit in usize (32 bit)
                    let len: usize = get_decimal_from_bytes(src)?.try_into()?;
                    // len + 2 to include the \r\n.
                    if src.remaining() < len + 2 {
                        return Err(Error::Incomplete);
                    }

                    let data = src.split_to(len);
                    // skip the \r\n
                    src.advance(2);

                    Ok(Frame::Bulk(data))
                }
            }
            b'*' => {
                if b'-' == peek_u8(src)? {
                    let line = get_line_from_bytes(src)?;
                    if *line != *b"-1" {
                        return Err("protocol error; invalid frame format".into());
                    }

                    Ok(Frame::Null)
                } else {
                    let len: usize = get_decimal_from_bytes(src)?.try_into()?;
                    let mut out_vec = Vec::with_capacity(len);

                    for _ in 0..len {
                        out_vec.push(Frame::parse(src)?);
                    }

                    Ok(Frame::Array(out_vec))
                }
            }
            b => {
                return Err(format!(
                    "protocol error; invalid frame format. Unexpected byte: {}",
                    b
                )
                .into());
            }
        }
    }

    /// Returns an empty array
    pub(crate) fn array() -> Frame {
        Frame::Array(vec![])
    }
}

/// Get byte at current cursor position without advancing the cursor.
fn peek_u8<T: Buf>(src: &mut T) -> Result<u8, Error> {
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
    if src.remaining() < n {
        // Don't have enough bytes yet.
        return Err(Error::Incomplete);
    }

    src.advance(n);
    Ok(())
}

/// Read a CRLF terminated decimal.
fn get_decimal(src: &mut Cursor<&[u8]>) -> Result<i64, Error> {
    let line = get_line(src)?;

    parse::extract_i64(line).ok_or_else(|| "protocol error; invalid frame format".into())
}

/// Read a CRLF terminated decimal.
fn get_decimal_from_bytes(src: &mut Bytes) -> Result<i64, Error> {
    let line = get_line_from_bytes(src)?;

    parse::extract_i64(&line).ok_or_else(|| "protocol error; invalid frame format".into())
}

/// Read a CRLF terminated double.
fn get_double(src: &mut Cursor<&[u8]>) -> Result<f64, Error> {
    let line = get_line(src)?;
    parse::extract_f64(line).ok_or_else(|| "protocol error; invalid frame format".into())
}

/// Read a CRLF terminated double.
fn get_double_from_bytes(src: &mut Bytes) -> Result<f64, Error> {
    let line = get_line_from_bytes(src)?;
    parse::extract_f64(&line).ok_or_else(|| "protocol error; invalid frame format".into())
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

/// Get all bytes until next CRLF.
fn get_line_from_bytes(src: &mut Bytes) -> Result<Bytes, Error> {
    let line_end = src
        .windows(2)
        .position(|window| window == b"\r\n")
        .ok_or_else(|| Error::Other("internal error: CRLF missing in pre-checked frame".into()))?;

    let line = src.split_to(line_end);
    // skip \r\n
    src.advance(2);

    Ok(line)
}

impl fmt::Display for Frame {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Frame::Error(err) => write!(fmt, "error: {err}"),
            Frame::Integer(num) => num.fmt(fmt),
            Frame::Double(num) => num.fmt(fmt),
            Frame::Bulk(msg) | Frame::Simple(msg) => match str::from_utf8(&msg) {
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

impl From<Data> for Frame {
    fn from(src: Data) -> Frame {
        match src {
            Data::Integer(val) => Frame::Integer(val),
            Data::Bytes(val) => Frame::Bulk(val),
            Data::String(val) => Frame::Simple(val),
            Data::Double(val) => Frame::Double(val),
            // NOTE: This will flatten nested array's.
            Data::Array(arr) => {
                let mut frame = Frame::array();
                for item in arr.iter() {
                    frame.push(Frame::from(item.clone()));
                }
                frame
            }
        }
    }
}

impl From<FromUtf8Error> for Error {
    fn from(_src: FromUtf8Error) -> Error {
        "protocol error; invalid framte format".into()
    }
}

impl TryFrom<Frame> for Data {
    type Error = String;
    fn try_from(src: Frame) -> Result<Self, Self::Error> {
        match src {
            Frame::Simple(string) => Ok(Data::String(string)),
            Frame::Bulk(bytes) => Ok(Data::Bytes(bytes)),
            Frame::Integer(val) => Ok(Data::Integer(val)),
            Frame::Double(val) => Ok(Data::Double(val)),
            // NOTE: This will flatten nested arrays.
            Frame::Array(arr) => {
                let mut data_vec = VecDeque::with_capacity(arr.len());
                for frame in arr.into_iter() {
                    data_vec.push_back(Data::try_from(frame)?);
                }
                Ok(Data::Array(data_vec))
            }
            Frame::Error(err) => Err(err.into()),
            Frame::Null => Err("Null not allowed for DB value.".into()),
        }
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
