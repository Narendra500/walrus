mod ping;
pub use ping::Ping;

mod set;
pub use set::Set;

use crate::{connection::Connection, frame::Frame, parse::Parse};

pub enum Command {
    Ping(Ping),
    Unknown(String),
}

impl Command {
    /// Parse a command from a frame.
    /// `Frame` must be of type Frame::Array(Frame)
    pub fn from_frame(frame: Frame) -> Result<Command, crate::Error> {
        // Convert the frame into a frame iterator using `Parse`.
        let mut parse = Parse::new(frame)?;

        // Command names are case insensitive, hence given command is converted to lowercase.
        let command_name = parse.next_string()?.to_lowercase();

        let command = match &command_name[..] {
            "ping" => Command::Ping(Ping::parse_frames(&mut parse)?),
            _ => Command::Unknown(command_name),
        };

        Ok(command)
    }

    /// Execute the command.
    ///
    /// The response is sent to client.
    pub(crate) async fn execute(self, conn: &mut Connection) -> Result<(), crate::Error> {
        match self {
            Command::Ping(cmd) => cmd.execute(conn).await,
            Command::Unknown(cmd) => {
                let response = Frame::Error(format!("ERR unknown command {cmd}"));
                conn.write_frame(&response).await?;
                Ok(())
            }
        }
    }
}
