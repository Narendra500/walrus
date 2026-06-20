mod ping;
pub use ping::Ping;

mod set;
pub use set::Set;

mod get;
pub use get::Get;

mod rpush;
pub use rpush::RPush;

mod lpush;
pub use lpush::LPush;

mod lpop;
pub use lpop::LPop;

mod blpop;
pub use blpop::BLPop;

mod llen;
pub use llen::LLen;

mod lrange;
pub use lrange::LRange;

mod wtype;
pub use wtype::Type;

use crate::{connection::Connection, db::Db, errors::WalrusError, frame::Frame, parse::Parse};

pub(crate) enum Command {
    Ping(Ping),
    Set(Set),
    Get(Get),
    RPush(RPush),
    LPush(LPush),
    LPop(LPop),
    BLPop(BLPop),
    LLen(LLen),
    LRange(LRange),
    Type(Type),
    Unknown(String),
}

impl Command {
    /// Parse a command from a frame.
    /// `Frame` must be of type Frame::Array(Frame)
    pub fn from_frame(frame: Frame) -> Result<Command, WalrusError> {
        // Convert the frame into a frame iterator using `Parse`.
        let mut parse = Parse::new(frame)?;

        // Command names are case insensitive, hence the given command will be compared using
        // case-insensitive comparison.
        let command_name = parse.next_bytes()?;

        let command = if command_name.eq_ignore_ascii_case(b"ping") {
            Command::Ping(Ping::parse_frames(&mut parse)?)
        } else if command_name.eq_ignore_ascii_case(b"set") {
            Command::Set(Set::parse_frames(&mut parse)?)
        } else if command_name.eq_ignore_ascii_case(b"get") {
            Command::Get(Get::parse_frame(&mut parse)?)
        } else if command_name.eq_ignore_ascii_case(b"rpush") {
            Command::RPush(RPush::parse_frames(&mut parse)?)
        } else if command_name.eq_ignore_ascii_case(b"lpush") {
            Command::LPush(LPush::parse_frames(&mut parse)?)
        } else if command_name.eq_ignore_ascii_case(b"lpop") {
            Command::LPop(LPop::parse_frames(&mut parse)?)
        } else if command_name.eq_ignore_ascii_case(b"blpop") {
            Command::BLPop(BLPop::parse_frames(&mut parse)?)
        } else if command_name.eq_ignore_ascii_case(b"llen") {
            Command::LLen(LLen::parse_frames(&mut parse)?)
        } else if command_name.eq_ignore_ascii_case(b"lrange") {
            Command::LRange(LRange::parse_frame(&mut parse)?)
        } else if command_name.eq_ignore_ascii_case(b"type") {
            Command::Type(Type::parse_frames(&mut parse)?)
        } else {
            Command::Unknown(String::from_utf8_lossy(&command_name[..]).to_string())
        };

        Ok(command)
    }

    /// Execute the command.
    ///
    /// The response is sent to client.
    pub(crate) async fn execute(self, db: &Db, conn: &mut Connection) -> Result<(), WalrusError> {
        match self {
            Command::Ping(cmd) => cmd.execute(conn).await,
            Command::Set(cmd) => cmd.execute(db, conn).await,
            Command::Get(cmd) => cmd.execute(db, conn).await,
            Command::RPush(cmd) => cmd.execute(db, conn).await,
            Command::LPush(cmd) => cmd.execute(db, conn).await,
            Command::LPop(cmd) => cmd.execute(db, conn).await,
            Command::BLPop(cmd) => cmd.execute(db, conn).await,
            Command::LLen(cmd) => cmd.execute(db, conn).await,
            Command::LRange(cmd) => cmd.execute(db, conn).await,
            Command::Type(cmd) => cmd.execute(db, conn).await,
            Command::Unknown(cmd) => {
                let response = Frame::Error(format!("ERR unknown command {cmd}"));
                conn.write_frame(&response).await?;
                Ok(())
            }
        }
    }
}
