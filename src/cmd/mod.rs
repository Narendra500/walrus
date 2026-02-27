mod ping;
pub use ping::Ping;

mod set;
pub use set::Set;

mod get;
pub use get::Get;

mod rpush;
pub use rpush::RPush;

use crate::{connection::Connection, db::Db, frame::Frame, parse::Parse};

pub enum Command {
    Ping(Ping),
    Set(Set),
    Get(Get),
    RPush(RPush),
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
            "set" => Command::Set(Set::parse_frames(&mut parse)?),
            "get" => Command::Get(Get::parse_frame(&mut parse)?),
            "rpush" => Command::RPush(RPush::parse_frames(&mut parse)?),
            _ => Command::Unknown(command_name),
        };

        Ok(command)
    }

    /// Execute the command.
    ///
    /// The response is sent to client.
    pub(crate) async fn execute(self, db: &Db, conn: &mut Connection) -> Result<(), crate::Error> {
        match self {
            Command::Ping(cmd) => cmd.execute(conn).await,
            Command::Set(cmd) => cmd.execute(db, conn).await,
            Command::Get(cmd) => cmd.execute(db, conn).await,
            Command::RPush(cmd) => cmd.execute(db, conn).await,
            Command::Unknown(cmd) => {
                let response = Frame::Error(format!("ERR unknown command {cmd}"));
                conn.write_frame(&response).await?;
                Ok(())
            }
        }
    }
}
