//! The lock around each shard.

use parking_lot::{Mutex, MutexGuard};

/// Times a contended lock spins before it starts yielding.
const SPINS: u32 = 128;
/// Times it then yields before it parks the thread.
const YIELDS: u32 = 16;

/// A mutex that spins, then yields, before it parks. Shard locks are held very briefly, but a
/// parked thread can wait a whole scheduler quantum to wake, stalling every connection on its
/// event loop. Long holds, such as a full sync copying a shard, still park their waiters.
pub(crate) struct ShardLock<T>(Mutex<T>);

impl<T> ShardLock<T> {
    pub fn new(value: T) -> Self {
        Self(Mutex::new(value))
    }

    #[inline]
    pub fn lock(&self) -> MutexGuard<'_, T> {
        match self.0.try_lock() {
            Some(guard) => guard,
            None => self.lock_contended(),
        }
    }

    #[cold]
    fn lock_contended(&self) -> MutexGuard<'_, T> {
        for _ in 0..SPINS {
            std::hint::spin_loop();
            // Reading first keeps waiters from taking the cache line from the holder.
            if !self.0.is_locked()
                && let Some(guard) = self.0.try_lock()
            {
                return guard;
            }
        }
        for _ in 0..YIELDS {
            std::thread::yield_now();
            if let Some(guard) = self.0.try_lock() {
                return guard;
            }
        }
        self.0.lock()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;

    #[test]
    fn contended_increments_are_all_kept() {
        let lock = Arc::new(ShardLock::new(0u64));
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let lock = lock.clone();
                std::thread::spawn(move || {
                    for _ in 0..100_000 {
                        *lock.lock() += 1;
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }
        assert_eq!(*lock.lock(), 800_000);
    }

    #[test]
    fn a_waiter_gets_a_lock_held_past_its_spinning() {
        let lock = Arc::new(ShardLock::new(0u64));
        let guard = lock.lock();
        let waiter = {
            let lock = lock.clone();
            std::thread::spawn(move || *lock.lock() + 1)
        };
        // Long enough that the waiter has spun and yielded and is parked.
        std::thread::sleep(Duration::from_millis(50));
        drop(guard);
        assert_eq!(waiter.join().unwrap(), 1);
    }
}
