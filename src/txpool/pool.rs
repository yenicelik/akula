#![allow(unreachable_code)]
use crate::{
    kv::{mdbx::MdbxTransaction, tables, MdbxWithDirHandle},
    models::{MessageWithSignature, U256},
    p2p::{
        node::Node,
        types::{
            GetPooledTransactions, InboundMessage, Message, NewPooledTransactionHashes, PeerFilter,
            PenaltyKind, PooledTransactions, Transactions,
        },
    },
    txpool::{types::*, PoolBuilder},
    TaskGuard,
};
use derive_more::Display;
use mdbx::{EnvironmentKind, TransactionKind, WriteMap, RO};
use parking_lot::Mutex;
use rand::Rng;
use std::{
    collections::BTreeSet,
    sync::{atomic::AtomicU64, Arc},
    time::Duration,
};
use task_group::TaskGroup;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tracing::*;

pub const PER_SENDER: usize = 32;

#[derive(Default)]
pub struct SharedState {
    lookup: TransactionLookup,

    best_queue: BTreeSet<ScoredTransaction>,
    worst_queue: BTreeSet<ScoredTransaction>,
}

pub struct Pool {
    pub node: Arc<Node>,
    pub db: Arc<MdbxWithDirHandle<WriteMap>>,

    state: Arc<Mutex<SharedState>>,

    min_priority_fee: U256,
}

impl Pool {
    pub fn builder() -> PoolBuilder {
        PoolBuilder::default()
    }
}

impl Pool {
    pub fn add_transaction<'env, 'txn>(
        &self,
        shared_state: &mut SharedState,
        txn: &'txn MdbxTransaction<'env, RO, WriteMap>,
        tx: Transaction,
        current_base_fee: U256,
    ) -> anyhow::Result<InsertionStatus>
    where
        'env: 'txn,
    {
        if shared_state.lookup.contains_hash(&tx.hash) {
            return Ok(InsertionStatus::Discarded(DiscardReason::AlreadyKnown));
        };

        let account = txn.get(tables::Account, tx.sender)?.unwrap_or_default();
        if account.nonce > tx.nonce() {
            return Ok(InsertionStatus::Discarded(DiscardReason::NonceTooLow));
        };
        if account.balance < tx.total_price() {
            return Ok(InsertionStatus::Discarded(
                DiscardReason::InsufficientBalance,
            ));
        }
        if tx.max_priority_fee_per_gas() < self.min_priority_fee {
            return Ok(InsertionStatus::Discarded(DiscardReason::PriorityFeeTooLow));
        }

        let scored_transaction = ScoredTransaction::new(&tx);
        let queue_type = if tx
            .max_fee_per_gas()
            .saturating_sub(tx.max_priority_fee_per_gas())
            >= current_base_fee
        {
            QueueType::Best
        } else {
            QueueType::Worst
        };
        shared_state.lookup.insert(tx);

        match queue_type {
            QueueType::Best => {
                shared_state.best_queue.insert(scored_transaction);
            }
            QueueType::Worst => {
                shared_state.worst_queue.insert(scored_transaction);
            }
        }
        Ok(InsertionStatus::Inserted(queue_type))
    }
}

impl Pool {
    pub async fn run(self: Arc<Self>) {
        let tasks = TaskGroup::new();

        let (request_tx, mut request_rx) = mpsc::channel::<(Vec<_>, _)>(128);
        tasks.spawn_with_name("transaction_requester", {
            let mut local_tasks = Vec::with_capacity(128);

            let this = self.node.clone();

            async move {
                while let Some((hashes, pred)) = request_rx.recv().await {
                    local_tasks.push(TaskGuard(tokio::spawn({
                        let this = this.clone();

                        async move {
                            let request_id = rand::thread_rng().gen::<u64>();

                            trace!(
                                "Sending transactions request: id={} len={} peer_predicate={:?}",
                                request_id,
                                hashes.len(),
                                pred
                            );

                            this.get_pooled_transactions(request_id, &hashes, pred)
                                .await;
                        }
                    })));
                }
            }
        });

        let (penalty_tx, mut penalty_rx) = mpsc::channel(128);
        tasks.spawn_with_name("peer_penalizer", {
            let this = self.node.clone();

            async move {
                while let Some(peer_id) = penalty_rx.recv().await {
                    let _ =
                        tokio::time::timeout(Duration::from_secs(2), this.penalize_peer(peer_id))
                            .await;
                }
            }
        });

        let (inbound_tx, mut inbound_rx) = mpsc::channel(128);
        tasks.spawn({
            let this = self.clone();

            async move {
                while let Some((GetPooledTransactions { request_id, hashes }, peer_id, sentry_id)) =
                    inbound_rx.recv().await
                {
                    todo!()
                    // let transactions = hashes
                    //     .iter()
                    //     .filter_map(|hash| {
                    //         this.by_hash
                    //             .get(hash)
                    //             .map(|shared_ref| shared_ref.clone().msg)
                    //     })
                    //     .collect::<Vec<_>>();
                    // this.node
                    //     .send_pooled_transactions(
                    //         request_id,
                    //         transactions,
                    //         PeerFilter::Peer(peer_id, sentry_id),
                    //     )
                    //     .await;
                }
            }
        });

        let (base_fee_tx, mut base_fee_rx) = mpsc::channel::<U256>(128);

        let (processor_tx, mut processor_rx) = mpsc::channel(128);
        tasks.spawn({
            let this = self.clone();
            let mut base_fee = loop {
                todo!()
            };

            async move {
                tokio::select! {
                    Some( transactions) = processor_rx.recv() => {
                        let txn = this.db.begin()?;
                        let mut shared_state = this.state.lock();

                        for transaction in transactions {
                            Self::add_transaction(&this, &mut shared_state, &txn, transaction, base_fee);
                        }
                    },
                    Some(new_base_fee) = base_fee_rx.recv() => { base_fee = new_base_fee; },

                }

                Ok::<_, anyhow::Error>(())
            }
        });

        tasks.spawn_with_name("incoming router", {
            let this = self.clone();

            async move {
                let mut stream = this.node.stream_transactions().await;

                while let Some(InboundMessage {
                    msg,
                    peer_id,
                    sentry_id,
                }) = stream.next().await
                {
                    match msg {
                        Message::NewPooledTransactionHashes(NewPooledTransactionHashes(hashes)) => {
                            request_tx
                                .send((hashes, PeerFilter::Peer(peer_id, sentry_id)))
                                .await?;
                        }
                        Message::Transactions(Transactions(transactions))
                            if !transactions.is_empty() =>
                        {
                            match transactions
                                .into_iter()
                                .map(Transaction::try_from)
                                .collect::<Result<Vec<_>, _>>()
                            {
                                Ok(transactions) => {
                                    processor_tx.send(transactions).await?;
                                }
                                Err(_) => {
                                    penalty_tx.send(peer_id).await?;
                                }
                            }
                        }
                        Message::GetPooledTransactions(request) => {
                            inbound_tx.send((request, peer_id, sentry_id)).await?;
                        }
                        Message::PooledTransactions(PooledTransactions {
                            transactions, ..
                        }) => {
                            match transactions
                                .into_iter()
                                .map(Transaction::try_from)
                                .collect::<Result<Vec<_>, _>>()
                            {
                                Ok(transactions) => {
                                    processor_tx.send(transactions).await?;
                                }
                                Err(_) => {
                                    penalty_tx.send(peer_id).await?;
                                }
                            }
                        }
                        _ => {}
                    }
                }

                Ok::<_, anyhow::Error>(())
            }
        });

        tasks.spawn({
            let this = self.clone();
            let mut ticker = tokio::time::interval(Duration::from_secs(1));

            async move {
                tokio::select! {
                    _ = ticker.tick() => {

                        // let unprocessed = this.unprocessed.lock().drain().collect::<Vec<_>>();
                        // for (tx, _) in unprocessed {
                        //     this.by_sender.entry(tx.sender).or_default().map(|list| {
                        //         if list.is_full() {
                        //             list.clear();
                        //         }
                        //         list.push(tx);
                        //         list
                        //     });
                        //     this.by_hash.insert(tx.hash, tx);


                        // }
                    }
                }
            }
        });

        std::future::pending::<()>().await;
    }
}
