use std::io::{self, Cursor};

use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::errors::WalrusError;
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
#[derive(Debug)]
pub struct Connection {
    stream: TcpStream,
    // Buffer for reading frames.
    buffer: BytesMut,
    // Buffer for writing frames.
    write_buffer: BytesMut,
}

impl Connection {
    /// create a new `Connection` to read and write to and from `TcpStream` using read and write
    /// buffers. The default initial size for the buffers is 16KB.
    /// There is no hard limit on how large the buffers can get.
    ///
    /// example:
    ///
    /// let socket = TcpStream::connect("127.0.0.1:6379").await?;
    ///
    /// let conn = Connection::new(socket, Some(32), Some(32));
    /// // intializes a new `Connection` with 32KB initial read and write buffers.
    pub fn new(
        socket: TcpStream,
        read_buffer_size: Option<u16>,
        write_buffer_size: Option<u16>,
    ) -> Connection {
        Connection {
            stream: socket,
            // defaults to 16KB buffers.
            buffer: BytesMut::with_capacity(read_buffer_size.unwrap_or(16) as usize * 1024),
            write_buffer: BytesMut::with_capacity(write_buffer_size.unwrap_or(16) as usize * 1024),
        }
    }

    /// Loops until enough data is available to read a frame from the buffer.
    /// Any remaining data is left untouched for next `read_frame`.
    ///
    /// Returns the frame parsed from `parse_frame` if frame is read successfuly
    /// else if connection is closed such that buffer was empty (no broken frame)
    /// then `None` is returned. Otherwise `Error` is returned.
    pub async fn read_frame(&mut self) -> Result<Option<Frame>, WalrusError> {
        loop {
            // Try to parse a frame. If enough data is buffered a frame is returned.
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }

            // Not enough buffered data to parse a full frame.
            // flush the current contents of the buffer to stream.
            if !self.write_buffer.is_empty() {
                self.stream.write_all(&self.write_buffer).await?;
                self.write_buffer.clear();
            }

            // Wait for client to send more data
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
    pub fn parse_frame(&mut self) -> Result<Option<Frame>, WalrusError> {
        // Wrap the cursor in buffer to track current location in the buffer.
        // Location starts from 0 when new cursor instance is created.
        let mut buf = Cursor::new(&self.buffer[..]);

        // Parse the frame, necessary datastructures are allocated and frame
        // is returned.
        //
        // If the encoded frame is invalid, an error is returned.
        match Frame::check(&mut buf) {
            // Full frame is available to parse.
            // len is inclusive of \r\n
            Ok(len) => {
                let frame_data = self.buffer.split_to(len);
                let mut frozen_data = frame_data.freeze();
                let frame = Frame::parse(&mut frozen_data)?;
                return Ok(Some(frame));
            }
            // Not enough data in the buffer to parse a full frame. More data must arrive
            // from the socket.
            //
            // Err is not returned as `Incomplete` 'error' is expected during the application
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
                self.write_buffer.put_u8(b'*');
                self.write_decimal(val.len() as i64).await?;

                let iter = val.iter();

                for frame in iter {
                    self.write_val(frame).await?;
                }
            }
            // frame is a literal. Encode using helper function for writing frame literals to the
            // stream.
            _ => self.write_val(frame).await?,
        }

        Ok(())
    }

    /// Write a frame literal (non array) to the stream.
    pub async fn write_val(&mut self, frame: &Frame) -> io::Result<()> {
        match frame {
            Frame::Simple(message) => {
                self.write_buffer.put_u8(b'+');
                self.write_buffer.put_slice(&message);
                self.write_buffer.put_slice(b"\r\n");
            }
            Frame::Error(err) => {
                self.write_buffer.put_u8(b'-');
                self.write_buffer.put_slice(err.as_bytes());
                self.write_buffer.put_slice(b"\r\n");
            }
            Frame::Integer(val) => {
                self.write_buffer.put_u8(b':');
                self.write_decimal(*val).await?;
            }
            Frame::Double(val) => {
                self.write_double(*val).await?;
            }
            Frame::Null => {
                self.write_buffer.put_slice(b"$-1\r\n");
            }
            Frame::Bulk(message) => {
                let message_len = message.len();

                self.write_buffer.put_u8(b'$');
                self.write_decimal(message_len as i64).await?;
                self.write_buffer.put_slice(message);
                self.write_buffer.put_slice(b"\r\n");
            }
            Frame::Array(_) => unreachable!(),
        }
        Ok(())
    }

    /// Write a double value to the stream.
    pub async fn write_double(&mut self, val: f64) -> io::Result<()> {
        use ryu;
        // RESP3 Special cases: +inf, -inf, nan
        if val.is_infinite() {
            if val.is_sign_positive() {
                self.write_buffer.put_slice(b",inf\r\n");
            } else {
                self.write_buffer.put_slice(b"-inf\r\n");
            }
            return Ok(());
        } else if val.is_nan() {
            self.write_buffer.put_slice(b",nan\r\n");
            return Ok(());
        }

        // Identifier for double.
        self.write_buffer.put_u8(b',');

        // Use ryu crate for better performance than format!() or to_string() method.
        // Uses a stack allocated buffer to avoid heap allocations.
        let mut buffer = ryu::Buffer::new();
        let printed: &str = buffer.format(val);

        self.write_buffer.put_slice(printed.as_bytes());
        self.write_buffer.put_slice(b"\r\n");

        Ok(())
    }

    /// Writes a decimal frame to the stream.
    pub async fn write_decimal(&mut self, val: i64) -> io::Result<()> {
        use itoa;
        // using itoa crate for better performance than std::fmt
        let mut buf = itoa::Buffer::new();
        // returns a reference to string representation of the number in the buffer.
        let printed = buf.format(val);

        self.write_buffer.put_slice(printed.as_bytes());
        self.write_buffer.put_slice(b"\r\n");

        Ok(())
    }
}
