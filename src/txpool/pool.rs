use crate::{
    kv::{mdbx::MdbxTransaction, tables, MdbxWithDirHandle},
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
use mdbx::{WriteMap, RO};
use parking_lot::Mutex;
use rand::Rng;
use std::{collections::BTreeSet, sync::Arc, time::Duration};
use task_group::TaskGroup;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tracing::*;

pub const PER_SENDER: usize = 32;

pub type Score = usize;

#[derive(Default)]
pub struct SharedState {
    lookup: TransactionLookup,

    best_queue: BTreeSet<Score>,
    worst_queue: BTreeSet<Score>,
}

impl SharedState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_transaction<'env, 'txn>(
        &mut self,
        txn: &'txn MdbxTransaction<'env, RO, WriteMap>,
        tx: Transaction,
    ) -> anyhow::Result<()>
    where
        'env: 'txn,
    {
        if self.lookup.contains_hash(tx.hash) {
            return Ok(());
        }

        let account = txn.get(tables::Account, tx.sender)?.unwrap_or_default();
        if account.nonce > tx.nonce() {
            // Just ignore this transaction if the sender's nonce is higher than the transaction's nonce.
            return Ok(());
        };

        account.balance;

        // TODO: check sender's balance.

        self.lookup.insert(tx);

        Ok(())
    }
}

pub struct Pool {
    pub node: Arc<Node>,
    pub db: Arc<MdbxWithDirHandle<WriteMap>>,

    state: Arc<Mutex<SharedState>>,
}

impl Pool {
    pub fn builder() -> PoolBuilder {
        PoolBuilder::default()
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
                while let Some((peer_id, reason)) = penalty_rx.recv().await {
                    trace!("Penalizing: peer={:?} reason={:?}", peer_id, reason);

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

        let (processor_tx, mut processor_rx) = mpsc::channel(128);
        tasks.spawn({
            let this = self.clone();

            async move {
                while let Some(transactions) = processor_rx.recv().await {
                    let txn = this.db.begin()?;
                    let mut shared_state = this.state.lock();

                    for transaction in transactions {
                        SharedState::add_transaction(&mut shared_state, &txn, transaction);
                    }
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
                                    penalty_tx
                                        .send((peer_id, PenaltyKind::MalformedTransaction))
                                        .await?;
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
                                    penalty_tx
                                        .send((peer_id, PenaltyKind::MalformedTransaction))
                                        .await?;
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
