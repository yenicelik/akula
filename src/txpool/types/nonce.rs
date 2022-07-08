use crate::{
    kv::{tables, MdbxWithDirHandle},
    models::H160,
};
use hashlink::{linked_hash_map::Entry as LruEntry, LruCache};
use parking_lot::Mutex;
use std::sync::Arc;

pub struct NonceTracker {
    /// Fallback, if nonce is not found in the cache.
    db: Arc<MdbxWithDirHandle<mdbx::WriteMap>>,

    /// Pending state nonce cache.
    cache: Arc<Mutex<LruCache<H160, u64>>>,
}

impl NonceTracker {
    pub fn new(db: Arc<MdbxWithDirHandle<mdbx::WriteMap>>) -> Self {
        Self {
            db,
            cache: Arc::new(Mutex::new(LruCache::new(1024))),
        }
    }

    pub fn get(&self, address: H160) -> u64 {
        let mut cache = self.cache.lock();

        match cache.entry(address) {
            LruEntry::Occupied(nonce) => *nonce.get(),
            LruEntry::Vacant(entry) => {
                let nonce = self
                    .db
                    .begin()
                    .expect("Failed to begin transaction")
                    .get(tables::Account, address)
                    .expect("Failed to get account")
                    .map(|account| account.nonce)
                    .unwrap_or(0);
                entry.insert(nonce);
                nonce
            }
        }
    }

    pub fn set(&self, address: H160, nonce: u64) {
        let mut cache = self.cache.lock();
        match cache.entry(address) {
            LruEntry::Occupied(mut entry) => {
                *entry.get_mut() = nonce;
            }
            LruEntry::Vacant(entry) => {
                entry.insert(nonce);
            }
        }
    }

    pub fn increment(&self, address: H160) {
        let mut cache = self.cache.lock();
        match cache.entry(address) {
            LruEntry::Occupied(mut entry) => {
                *entry.get_mut() += 1;
            }
            LruEntry::Vacant(entry) => {
                let nonce = self
                    .db
                    .begin()
                    .expect("Failed to begin transaction")
                    .get(tables::Account, address)
                    .expect("Failed to get account")
                    .map(|account| account.nonce)
                    .unwrap_or(0);
                entry.insert(nonce + 1);
            }
        }
    }

    #[inline]
    pub fn reset(&self) {
        self.cache.lock().clear();
    }
}
