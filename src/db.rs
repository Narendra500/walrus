use ahash;
use bytes::Bytes;
use dashmap::{
    DashMap,
    mapref::one::{Ref, RefMut},
};
use futures::{StreamExt, stream::FuturesUnordered};
use std::{
    collections::{BTreeSet, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::{
    sync::Notify,
    time::{self, Duration, Instant},
};

use crate::{errors::WalrusError, frame::Frame, parse};

/// Data stored in an entry.
/// Can be Bytes, Simple String or an Vec<Data>
#[derive(Clone, Debug, PartialEq)]
pub enum Data {
    Bytes(Bytes),
    /// VecDeque allowing O(1) push and pop operations at both ends of the list.
    Array(VecDeque<Data>),
    String(Bytes),
    Integer(i64),
    Double(f64),
}

/// Single entry in key-value store.
pub(crate) struct Entry {
    pub(crate) data: Data,
    pub(crate) expires_at: Option<Instant>,
}

/// State of the Db.
struct State {
    /// Dashmap using ahash hashing algorithm providing better performance compared to SipHash.
    entries: DashMap<Bytes, Entry, ahash::RandomState>,

    /// Tracks key's Time To Live.
    /// Binary Tree Set is used to the value expiring next.
    /// It is possible to have two values expire at same instant.
    /// A unique key is used to break these ties.
    /// std::sync::Mutex is used here as its cheaper to just wait for BTreeSet operation than wait
    /// for context switiching if using tokio::sync::Mutex
    expirations: Mutex<BTreeSet<(Instant, Bytes)>>,

    /// Indicates if Db instance is shutting down. Background tasks are signaled to exit
    /// when this is true.
    shutdown: AtomicBool,

    /// Map of keys to Notification triggers.
    blocking_keys: DashMap<Bytes, Arc<Notify>>,
}

/// Shared state.
struct Shared {
    state: State,
    /// Notifies the background task handling entry expiration.
    /// The background task waits to be notified, then checks for expired values
    /// or the shutdown signal.
    background_task: Notify,
}

/// Shared across all connections.
/// When `Db` instance is created a background task is created to expire values after the
/// requested duration has elapsed. This task terminates when `Db` instance is dropped.
#[derive(Clone)]
pub(crate) struct Db {
    shared: Arc<Shared>,
}

/// Wrapper around `Db` instance, allows for cleanup of the `Db` by signalling the background
/// purge task to shutdown when this struct is dropped.
pub(crate) struct DbDropGuard {
    db: Db,
}

impl Data {
    /// Try to convert `Frame` to `Vec<Data>`.
    pub(crate) fn frame_to_data_vec(frame: Frame) -> Result<Vec<Data>, WalrusError> {
        match frame {
            Frame::Array(arr) => arr
                .into_iter()
                .map(Data::try_from)
                .collect::<Result<Vec<_>, _>>()
                .map_err(Into::into),
            other => Ok(vec![Data::try_from(other)?]),
        }
    }
}

impl Db {
    /// Create a new empty `Db` instance.
    pub(crate) fn new() -> Db {
        let shared = Arc::new(Shared {
            state: State {
                entries: DashMap::with_capacity_and_hasher_and_shard_amount(
                    512,
                    ahash::RandomState::new(),
                    64,
                ),
                expirations: Mutex::new(BTreeSet::new()),
                shutdown: AtomicBool::new(false),
                blocking_keys: DashMap::new(),
            },
            background_task: Notify::new(),
        });

        // Start the background task for purging expired keys passing shared Db state.
        tokio::spawn(purge_expired_tasks(shared.clone()));

        Db { shared }
    }

    /// Get the value associated with a key.
    ///
    /// Returns `None` if no value is associated with the key.
    pub(crate) fn get(&self, key: &Bytes) -> Option<Data> {
        // clone here is shallow as data is stored using `Bytes`.
        self.shared
            .state
            .entries
            .get(key)
            .map(|entry| entry.data.clone())
    }

    pub(crate) fn get_mut(&self, key: &Bytes) -> Option<RefMut<'_, Bytes, Entry>> {
        self.shared.state.entries.get_mut(key)
    }

    pub(crate) fn get_ref(&self, key: &Bytes) -> Option<Ref<'_, Bytes, Entry>> {
        self.shared.state.entries.get(key)
    }

    /// Insert key value pair into db.
    /// Optional expires_at determines the instant when key will expire.
    /// If key already exists, its old value is replaced.
    pub(crate) fn set(&self, key: &Bytes, value: Data, expire: Option<Duration>) {
        let mut notify = false;
        // The `key` still refers to the Bytes from the BytesMut buffer, to avoid memory mapping copy
        // it before storing. `value` maybe owned already if its not bytes.
        let stored_key = Bytes::copy_from_slice(&key);
        let stored_value = value.to_owned();

        let expires_at = expire.map(|duration| {
            // Calculate the instant at which key will expire.
            let when = Instant::now() + duration;

            // Set notify to true if new key will expire earlier than current scheduled next
            // expiration.
            notify = self
                .shared
                .state
                .next_expiration()
                .map(|expiration| when < expiration)
                .unwrap_or(true);

            when
        });

        // Insert pair into dashmap, returns previous entry if key already present.
        let prev = self.shared.state.entries.insert(
            key.clone(),
            Entry {
                data: stored_value,
                expires_at,
            },
        );

        // If prev entry was present then remove its expiration to avoid data leak.
        if let Some(prev) = prev {
            if let Some(when) = prev.expires_at {
                self.shared
                    .state
                    .expirations
                    .lock()
                    .unwrap()
                    .remove(&(when, stored_key.clone()));
            }
        }

        // Track the expiration of new entry.
        if let Some(when) = expires_at {
            self.shared
                .state
                .expirations
                .lock()
                .unwrap()
                .insert((when, stored_key));
        }

        // Notify the background task if it needs to update its state to reflect new expiration.
        if notify {
            self.shared.background_task.notify_one();
        }
    }

    /// Pop the first element of an array.
    /// Returns `None` if the array is empty or key does not exist.
    /// Returns `Err` if key holds a non-array value.
    pub(crate) fn pop_front(&self, key: &Bytes) -> Result<Option<Data>, WalrusError> {
        let mut remove = false;
        let data = {
            let maybe_entry = self.shared.state.entries.get_mut(key);
            if let Some(mut entry) = maybe_entry {
                match entry.data {
                    Data::Array(ref mut arr) => {
                        let data = arr.pop_front();
                        if arr.is_empty() {
                            remove = true;
                        }
                        Ok(data)
                    }
                    _ => Err(WalrusError::WrongType),
                }
            } else {
                return Ok(None);
            }
        };

        if remove {
            self.shared.state.entries.remove(key);
        }

        data
    }

    /// Pop the last element of an array.
    /// Returns `None` if the array is empty or key does not exist.
    /// Returns `Err` if key holds a non-array value.
    pub(crate) fn pop_back(&self, key: &Bytes) -> Result<Option<Data>, WalrusError> {
        let mut remove = false;
        let data = {
            let maybe_entry = self.shared.state.entries.get_mut(key);
            if let Some(mut entry) = maybe_entry {
                match entry.data {
                    Data::Array(ref mut arr) => {
                        let data = arr.pop_back();
                        if arr.is_empty() {
                            remove = true;
                        }
                        Ok(data)
                    }
                    _ => Err(
                        "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                    ),
                }
            } else {
                return Ok(None);
            }
        };

        if remove {
            self.shared.state.entries.remove(key);
        }

        data
    }

    /// Notify a connection waiting on a key.
    pub(crate) fn notify_blocked(&self, key: &Bytes) {
        if let Some(notify) = self.shared.state.blocking_keys.get(key) {
            notify.notify_one();
        }
    }

    /// Get or create a notifier for a key.
    pub(crate) fn get_or_create_notifier(&self, key: &Bytes) -> Arc<Notify> {
        self.shared
            .state
            .blocking_keys
            .entry(key.clone())
            .or_insert_with(|| Arc::new(Notify::new()))
            .clone()
    }

    /// Signals the background task to shutdown.
    fn shutdown_purge_task(&self) {
        // Set state.shutdown to `true` signaling the background task to shutdown.
        // Ordering::Relaxed cause exact CPU instruction ordering relative to other variables
        // doesn't matter.
        self.shared.state.shutdown.store(true, Ordering::Relaxed);

        self.shared.background_task.notify_one();
    }
}

impl DbDropGuard {
    /// Create a new `DbDropGuard` instance, this wraps a `Db` instance.
    /// Dropping DbDropGuard will shutdown the `Db`'s background purge task.
    pub(crate) fn new() -> DbDropGuard {
        DbDropGuard { db: Db::new() }
    }

    /// Get the shared `Db`. Since Db has Arc internally -- cloning it is same as cloning
    /// Arc so it only increments the ref count.
    pub(crate) fn get_db(&self) -> Db {
        self.db.clone()
    }
}

impl Drop for DbDropGuard {
    fn drop(&mut self) {
        // Signal the `Db` instance to shutdown the background task that purges expired keys.
        self.db.shutdown_purge_task();
    }
}

impl State {
    /// Get the `Instant` of next expiration if any.
    fn next_expiration(&self) -> Option<Instant> {
        self.expirations
            .lock()
            .unwrap()
            .iter()
            .next()
            .map(|expiration| expiration.0)
    }
}

impl Shared {
    /// Purge all expired keys and return the `Instant` at which the next key will expire.
    /// Background task will sleep until this instant.
    fn purge_expired_keys(&self) -> Option<Instant> {
        if self.state.shutdown.load(Ordering::Relaxed) {
            // The database is shutting down. The background task should exit.
            return None;
        }

        // Find all keys scheduled to expire before `now`.
        let now = Instant::now();

        loop {
            let mut expirations = self.state.expirations.lock().unwrap();
            if let Some(&(when, ref key)) = expirations.iter().next() {
                if when > now {
                    // Done purging, `when` is the instant at which the next key will expire.
                    // The worker task will wait until this instant.
                    return Some(when);
                }

                let key_clone = key.clone();
                let when_clone = when;

                // Remove from expirations set first.
                expirations.remove(&(when_clone, key_clone.clone()));

                // Drop the lock before operating on DashMap entries to avoid deadlock.
                drop(expirations);

                // Remove the expired entry from DashMap.
                self.state.entries.remove(&key_clone);
            } else {
                return None;
            }
        }
    }

    /// Returns `true` if database is shutting down.
    fn is_shutdown(&self) -> bool {
        self.state.shutdown.load(Ordering::Relaxed)
    }
}

/// Executed by background tasks.
///
/// Wait to be notified. On notification purge any expired keys from the
/// shared state. If `shutdown` is set, terminate the task.
async fn purge_expired_tasks(shared: Arc<Shared>) {
    while !shared.is_shutdown() {
        // Purges all expired keys, the function returns the instant at which next
        // key will expire. The worker must wait until the instant has passed or is
        // notified.
        if let Some(when) = shared.purge_expired_keys() {
            tokio::select! {
                _ = time::sleep_until(when) => {},
                _ = shared.background_task.notified() => {},
            }
        } else {
            // No keys expiring in the future, wait to be notified.
            shared.background_task.notified().await;
        }
    }

    println!("Purge background task shutdown")
}

/// Wait on any of the notifiers to be notified.
pub(crate) async fn wait_on_any(notifiers: &[Arc<Notify>]) {
    let mut futures: FuturesUnordered<_> = notifiers.iter().map(|n| n.notified()).collect();

    if futures.is_empty() {
        return;
    }

    futures.next().await;
}

/// Takes Bytes and chooses the most optimal representation of the data.
/// Returns a `Data` instance.
///
/// # example
/// b'123' will be represented as `Data::Integer(123)`.
/// b'123.456' will be represented as `Data::Double(123.456)`.
pub(crate) fn optimize_storage(bytes: Bytes) -> Data {
    if let Some(i) = parse::extract_i64_strict(&bytes) {
        Data::Integer(i)
    } else if let Some(f) = parse::extract_f64(&bytes) {
        Data::Double(f)
    } else {
        Data::Bytes(bytes)
    }
}

/// Convert `i64` to `Bytes`.
/// Converts `i64` to a slice of bytes which are then copied into a `Bytes` instance.
pub(crate) fn int_to_bytes(val: i64) -> Bytes {
    let mut buf = itoa::Buffer::new();
    let printed = buf.format(val);
    Bytes::copy_from_slice(printed.as_bytes())
}

/// Convert `f64` to `Bytes`.
/// Converts `f64` to a slice of bytes which are then copied into a `Bytes` instance.
pub(crate) fn double_to_bytes(val: f64) -> Bytes {
    use ryu;
    // RESP3 Special cases: +inf, -inf, nan
    if val.is_infinite() {
        if val.is_sign_positive() {
            return Bytes::copy_from_slice(b"inf");
        } else {
            return Bytes::copy_from_slice(b"-inf");
        }
    } else if val.is_nan() {
        return Bytes::copy_from_slice(b"nan");
    }

    // Use ryu crate for better performance than format!() or to_string() method.
    // Uses a stack allocated buffer to avoid heap allocations.
    let mut buffer = ryu::Buffer::new();
    let printed: &str = buffer.format(val);

    Bytes::copy_from_slice(printed.as_bytes())
}
