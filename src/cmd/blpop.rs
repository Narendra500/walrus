use futures::FutureExt;
use std::{future::pending, sync::Arc, time::Duration};

use bytes::Bytes;
use tokio::{sync::Notify, time::sleep};

use crate::{
    Connection,
    db::{Db, wait_on_any},
    errors::WalrusError,
    frame::Frame,
};

/// BLPop command.
/// BLPOP key \[key ...\] timeout
///
/// BLPOP returns the name of the key that was popped and the corresponding value.
/// This command blocks until one of the keys can be popped (that is, exists and has a value) or
/// until the timeout is reached.
/// If timeout is reached, a Null reply is returned.
///
/// If multiple clients are blocked trying to pop the same key, the longest waiting client is
/// considered as higher priority and gets the key.
///
/// If multiple blocking keys have values pushed at the same time, the key specified first in the
/// list of keys will be popped.
#[derive(Debug)]
pub struct BLPop {
    keys: Vec<String>,
    timeout: u64,
}

impl BLPop {
    /// Returns a new BLPop command.
    pub fn new(keys: Vec<String>, timeout: u64) -> Self {
        Self { keys, timeout }
    }

    /// Parse the BLPop command from an array frame.
    /// 'BLPOP' string is already consumed.
    ///
    /// The array frame must contain at least three elements.
    /// BLPOP key [key ...] timeout
    pub(crate) fn parse_frames(parse: &mut crate::parse::Parse) -> Result<Self, WalrusError> {
        let (keys, timeout) = parse.next_strings_with_timeout()?;

        Ok(Self::new(keys, timeout))
    }

    /// Execute the BLPop command.
    /// Blocks until any of the keys can be popped or the timeout is reached.
    /// Always tries to pop the keys in the order they were specified.
    ///
    /// # Returns
    ///
    /// Array frame with the name of the key that was popped and the corresponding value.
    /// Null frame is returned if the timeout is reached.
    pub(crate) async fn execute(&self, db: &Db, conn: &mut Connection) -> Result<(), WalrusError> {
        let mut timer = if self.timeout > 0 {
            Box::pin(sleep(Duration::from_secs(self.timeout)).boxed())
        } else {
            // If timeout is 0, this future hangs forever.
            // disabling the timeout branch.
            Box::pin(pending().boxed())
        };

        loop {
            // Try LPOP for each key.
            for key in &self.keys {
                match db.pop_front(key) {
                    Ok(Some(data)) => {
                        conn.write_frame(&Frame::Array(vec![
                            Frame::Bulk(Bytes::from(key.clone())),
                            Frame::from(data),
                        ]))
                        .await?;
                        return Ok(());
                    }
                    Err(err) => {
                        conn.write_frame(&Frame::Error(err.get_msg().into()))
                            .await?;
                        return Err(err);
                    }
                    _ => {}
                }
            }

            // Get the notification receivers for all requested keys.
            let notifiers: Vec<Arc<Notify>> = self
                .keys
                .iter()
                .map(|key| db.get_or_create_notifier(key))
                .collect();

            // Block until either the timer or one of the keys is notified.
            tokio::select! {
                // The timer finished.
                _ = &mut timer, if self.timeout > 0 => {
                    conn.write_frame(&Frame::Null).await?;
                    return Ok(());
                }
                // A key was notified.
                _ = wait_on_any(&notifiers) => {
                    // Instead of popping the key, we loop again and safely acquire the DB lock and
                    // try to pop at the top.
                    continue;
                }
            }
        }
    }

    /// Convert the BLPop command into a frame.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push(Frame::Bulk(Bytes::from("BLPOP")));
        for key in self.keys {
            frame.push(Frame::Bulk(Bytes::from(key)));
        }
        frame.push(Frame::Integer(self.timeout as i64));

        frame
    }
}
