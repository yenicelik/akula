use crate::{
    models::{H160, H256},
    txpool::types::Transaction,
};
use hashbrown::{HashMap, HashSet};
use std::borrow::Borrow;

#[derive(Debug, Default)]
pub struct TransactionLookup {
    by_hash: HashMap<H256, Transaction>,
    by_sender: HashMap<H160, HashSet<H256>>,
}

impl TransactionLookup {
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn insert(&mut self, tx: Transaction) {
        let sender = tx.sender;
        let hash = tx.hash;
        self.by_hash.insert(hash, tx);
        self.by_sender.entry(sender).or_default().insert(hash);
    }

    #[inline]
    pub fn get<Q>(&self, hash: Q) -> Option<&Transaction>
    where
        Q: Borrow<H256>,
    {
        self.by_hash.get(hash.borrow())
    }

    #[inline]
    pub fn get_by_sender<'lookup, 'sender>(
        &'lookup self,
        sender: &'sender H160,
    ) -> Option<Vec<&'sender Transaction>>
    where
        'lookup: 'sender,
    {
        self.by_sender.get(sender).map(|hashes| {
            hashes
                .iter()
                .map(|hash| self.get(hash).unwrap())
                .collect::<Vec<_>>()
        })
    }

    #[inline]
    pub fn contains_hash<Q>(&self, hash: Q) -> bool
    where
        Q: Borrow<H256>,
    {
        self.by_hash.contains_key(hash.borrow())
    }

    #[inline]
    pub fn contains_sender<Q>(&self, sender: Q) -> bool
    where
        Q: Borrow<H160>,
    {
        self.by_sender.contains_key(sender.borrow())
    }

    #[inline]
    pub fn remove<Q>(&mut self, hash: Q) -> Option<Transaction>
    where
        Q: Borrow<H256>,
    {
        self.by_hash.remove(hash.borrow())
    }
}

impl Extend<Transaction> for TransactionLookup {
    #[inline]
    fn extend<T>(&mut self, iter: T)
    where
        T: IntoIterator<Item = Transaction>,
    {
        for tx in iter {
            self.insert(tx);
        }
    }
}
