use std::hash::Hash;

use crate::models::{MessageWithSignature, H160, H256, U256};
use derive_more::Deref;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueuedTranasction {
    pub hash: H256,
    pub sender: H160,
    pub nonce: u64,
    pub max_priority_fee_per_gas: U256,
    pub max_fee_per_gas: U256,
}

impl From<&Transaction> for QueuedTranasction {
    fn from(msg: &Transaction) -> Self {
        Self::new(msg)
    }
}

impl QueuedTranasction {
    pub fn new(msg: &Transaction) -> Self {
        Self {
            hash: msg.hash,
            sender: msg.sender,
            nonce: msg.nonce(),
            max_priority_fee_per_gas: msg.max_priority_fee_per_gas(),
            max_fee_per_gas: msg.max_fee_per_gas(),
        }
    }
}

impl Ord for QueuedTranasction {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        if self.sender == other.sender {
            match self.nonce.cmp(&other.nonce) {
                std::cmp::Ordering::Equal => self
                    .max_priority_fee_per_gas
                    .cmp(&other.max_priority_fee_per_gas),
                std::cmp::Ordering::Less => std::cmp::Ordering::Greater,
                std::cmp::Ordering::Greater => std::cmp::Ordering::Less,
            }
        } else {
            self.max_priority_fee_per_gas
                .cmp(&other.max_priority_fee_per_gas)
        }
    }
}

impl PartialOrd for QueuedTranasction {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deref)]
pub struct Transaction {
    #[deref]
    pub msg: MessageWithSignature,
    pub sender: H160,
    pub hash: H256,
}

impl Hash for Transaction {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.hash.hash(state)
    }
}

impl TryFrom<MessageWithSignature> for Transaction {
    type Error = anyhow::Error;

    fn try_from(msg: MessageWithSignature) -> Result<Self, Self::Error> {
        Self::new(msg)
    }
}

impl Transaction {
    pub fn new(msg: MessageWithSignature) -> anyhow::Result<Self> {
        let sender = msg.recover_sender()?;
        let hash = msg.hash();
        Ok(Self { msg, sender, hash })
    }
}
