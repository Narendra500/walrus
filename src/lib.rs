mod connection;
pub use connection::Connection;

pub mod cmd;
pub use cmd::Command;

pub mod frame;
pub mod parse;

pub mod server;

pub mod client;

pub mod db;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
