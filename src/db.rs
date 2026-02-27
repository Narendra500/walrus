use bytes::Bytes;
use std::{
    collections::{BTreeSet, HashMap},
    sync::{Arc, Mutex},
};
use tokio::{
    sync::Notify,
    time::{self, Duration, Instant},
};

/// Data stored in an entry.
/// Can be Bytes, Simple String or an Vec<Data>
#[derive(Clone)]
pub enum Data {
    Bytes(Bytes),
    Array(Vec<Data>),
    String(String),
    Integer(u64),
}

/// Single entry in key-value store.
struct Entry {
    data: Data,
    expires_at: Option<Instant>,
}

/// State of the Db.
struct State {
    entries: HashMap<String, Entry>,

    /// Tracks key's Time To Live.
    /// Binary Tree Set is used to the value expiring next.
    /// It is possible to have two values expire at same instant.
    /// A unique key is used to break these ties.
    expirations: BTreeSet<(Instant, String)>,

    /// Indicates if Db instance is shutting down. Background tasks are signaled to exit
    /// when this is true.
    shutdown: bool,
}

/// Shared state is wrapped in Mutex.
struct Shared {
    state: Mutex<State>,
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

impl Db {
    /// Create a new empty `Db` instance.
    pub(crate) fn new() -> Db {
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                entries: HashMap::new(),
                expirations: BTreeSet::new(),
                shutdown: false,
            }),
            background_task: Notify::new(),
        });

        // Start the background task for purging expired keys passing shared Db state.
        tokio::spawn(purge_expired_tasks(shared.clone()));

        Db { shared }
    }

    /// Get the value associated with a key.
    ///
    /// Returns `None` if no value is associated with the key.
    pub(crate) fn get(&self, key: &str) -> Option<Data> {
        let state = self.shared.state.lock().unwrap();
        // clone here is shallow as data is stored using `Bytes`.
        state.entries.get(key).map(|entry| entry.data.clone())
    }

    /// Insert key value pair into db.
    /// Optional expires_at determines the instant when key will expire.
    /// If key already exists, its old value is replaced.
    pub(crate) fn set(&self, key: String, value: Data, expire: Option<Duration>) {
        let mut state = self.shared.state.lock().unwrap();

        let mut notify = false;

        let expires_at = expire.map(|duration| {
            // Calculate the instant at which key will expire.
            let when = Instant::now() + duration;

            // Set notify to true if new key will expire earlier than current scheduled next
            // expiration.
            notify = state
                .next_expiration()
                .map(|expiration| when < expiration)
                .unwrap_or(true);

            when
        });

        // Insert pair into hashmap, returns previous entry if key already present.
        let prev = state.entries.insert(
            key.clone(),
            Entry {
                data: value,
                expires_at,
            },
        );

        // If prev entry was present then remove its expiration to avoid data leak.
        if let Some(prev) = prev {
            if let Some(when) = prev.expires_at {
                state.expirations.remove(&(when, key.clone()));
            }
        }

        // Track the expiration of new entry.
        if let Some(when) = expires_at {
            state.expirations.insert((when, key));
        }

        // Release the Mutex before notifying the background task.
        // Avoids background task waking up to acquire mutex that function is still holding.
        drop(state);

        // Notify the background task if it needs to update its state to reflect new expiration.
        if notify {
            self.shared.background_task.notify_one();
        }
    }

    /// Signals the background task to shutdown.
    fn shutdown_purge_task(&self) {
        // Set state.shutdown to `true` signaling the background task to shutdown.
        let mut state = self.shared.state.lock().unwrap();
        state.shutdown = true;

        // drop the lock before notifying the task.
        drop(state);
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
            .iter()
            .next()
            .map(|expiration| expiration.0)
    }
}

impl Shared {
    /// Purge all expired keys and return the `Instant` at which the next key will expire.
    /// Background task will sleep until this instant.
    fn purge_expired_keys(&self) -> Option<Instant> {
        let mut state = self.state.lock().unwrap();

        if state.shutdown {
            // The database is shutting down. The background task should exit.
            return None;
        }

        // For the borrow checker. `lock` returns `MutexGuard` and not a &mut State.
        // The borrow checker can't check that it is safe to access `state.entries` and
        // `state.expirations` mutably through the mutex guard.
        // Hence a mutable reference to `State` is acquired outside the loop.
        let state = &mut *state;

        // Find all keys scheduled to expire before `now`.
        let now = Instant::now();

        while let Some(&(when, ref key)) = state.expirations.iter().next() {
            if when > now {
                // Done purging, `when` is the instant at which the next key will expire.
                // The worker task will wait until this instant.
                return Some(when);
            }
            // remove the expired entry from HashMap.
            state.entries.remove(key);
            state.expirations.remove(&(when, key.clone()));
        }

        None
    }

    /// Returns `true` if database is shutting down.
    fn is_shutdown(&self) -> bool {
        self.state.lock().unwrap().shutdown
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
