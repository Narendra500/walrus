use std::io::{self, Cursor};

use bytes::{Buf, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;

use crate::frame::Frame;

/// Send and receive `Frame` values from a remote peer.
///
/// To read frames, `Connection` uses internal buffer wrapped in `BufWriter`
/// for efficient writes to the buffer in batches. The buffer is filled with
/// enough bytes to create a full frame. Then `Connection` creates a frame
/// and returns it to the caller.
///
/// To send frames, the frame is first encoded into the write buffer.
/// The contents of the write buffer are then written to the socket.
pub struct Connection {
    stream: BufWriter<TcpStream>,
    // The buffer for reading frames.
    buffer: BytesMut,
}

impl Connection {
    /// create a new `Connection`, wraps socket in `BufWriter` and initializes a read buffer of
    /// type `BytesMut` with default capacity of 16KB.
    ///
    /// example:
    ///
    /// let socket = TcpStream::connect("127.0.0.1:6379").await?;
    ///
    /// let conn = Connection::new(socket, Some(32));
    /// // intializes a new `Connection` with 32KB read buffer.
    pub fn new(socket: TcpStream, capacity: Option<usize>) -> Connection {
        Connection {
            stream: BufWriter::new(socket),
            // defaults to 16KB read buffer.
            buffer: BytesMut::with_capacity(capacity.unwrap_or(16) * 1024),
        }
    }

    /// Loops until enough data is available to read a frame from the buffer.
    /// Any remaining data is left untouched for next `read_frame`.
    ///
    /// Returns the frame parsed from `parse_frame` if frame is read successfuly
    /// else if connection is closed such that buffer was empty (no broken frame)
    /// then `None` is returned. Otherwise `Error` is returned.
    pub async fn read_frame(&mut self) -> Result<Option<Frame>, crate::Error> {
        loop {
            // Try to parse a frame. If enough data is buffered a frame is returned.
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }

            // Not enough buffered data to parse the frame, Try to read more from the
            // socket.
            //
            // If number of bytes read into buffer is 0, then the stream has ended.
            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                // If the stream ended with no data in the buffer it is a clean shutdown.
                // Else it ended while sending a frame.
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err("Connection reset by peer".into());
                }
            }
        }
    }

    /// Tries to parse a frame from the buffer. Parsed data is returned and
    /// removed from buffer. Ok(None) is returned if not enough data is buffered
    /// yet. Err is returned in case of invalid frame format.
    pub fn parse_frame(&mut self) -> Result<Option<Frame>, crate::Error> {
        // Wrap the cursor in buffer to track current location in the buffer.
        // Location starts from 0 when new cursor instance is created.
        let mut buf = Cursor::new(&self.buffer[..]);

        // First check if a frame can be parsed.
        match Frame::check(&mut buf) {
            Ok(_) => {
                // The check function advances the cursor position to the end of
                // the frame. Since the position starts from 0, len of the frame is
                // current position. The position is <message>\r\n<HERE>.
                let len = buf.position() as usize;

                // set cursor position back to 0 before parsing the frame.
                buf.set_position(0);

                // Parse the frame, necessary datastructures are allocated and frame
                // is returned.
                //
                // If the encoded frame is invalid, an error is returned.
                let frame = Frame::parse(&mut buf)?;

                // Advance the internal 'cursor' of the ByteMut buffer to discard the
                // parsed data.
                self.buffer.advance(len);

                Ok(Some(frame))
            }
            // Not enough data in the buffer to parse a full frame. More data must arrive
            // from the socket.
            //
            // Err is not returned as as `Incomplete` 'error' is expected during the application
            // runtime.
            Err(crate::frame::Error::Incomplete) => Ok(None),
            // An unexpected error occured while parsing the frame. The connection will be closed.
            Err(e) => Err(e.into()),
        }
    }

    /// Write a single `Frame` to the stream.
    ///
    /// Nested array's not supported as of yet.
    pub async fn write_frame(&mut self, frame: &Frame) -> io::Result<()> {
        match frame {
            Frame::Array(val) => {
                self.stream.write_u8(b'*').await?;
                self.write_decimal(val.len() as u64).await?;

                let iter = val.iter();

                for frame in iter {
                    self.write_val(frame).await?;
                }
            }
            // frame is a literal. Encode using helper function for writing frame literals to the
            // stream.
            _ => self.write_val(frame).await?,
        }

        // The writes above are to the buffered stream. `flush` writes the remaining contents
        // of the buffer to the socket.
        self.stream.flush().await
    }

    /// Write a frame literal (non array) to the stream.
    pub async fn write_val(&mut self, frame: &Frame) -> io::Result<()> {
        match frame {
            Frame::Simple(message) => {
                self.stream.write_u8(b'+').await?;
                self.stream.write_all(message.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Error(err) => {
                self.stream.write_u8(b'-').await?;
                self.stream.write_all(err.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Integer(val) => {
                self.stream.write_u8(b':').await?;
                self.write_decimal(*val).await?;
            }
            Frame::Null => {
                self.stream.write_all(b"$-1\r\n").await?;
            }
            Frame::Bulk(message) => {
                let message_len = message.len();

                self.stream.write_u8(b'$').await?;
                self.write_decimal(message_len as u64).await?;
                self.stream.write_all(message).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Array(_) => unreachable!(),
        }
        Ok(())
    }

    /// Writes a decimal frame to the stream.
    pub async fn write_decimal(&mut self, val: u64) -> io::Result<()> {
        use itoa;
        // using itoa crate for better performance than std::fmt
        let mut buf = itoa::Buffer::new();
        // returns a reference to string representation of the number in the buffer.
        let printed = buf.format(val);

        self.stream.write_all(printed.as_bytes()).await?;
        self.stream.write_all(b"\r\n").await?;

        Ok(())
    }
}
