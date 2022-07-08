use crate::{
    models::{H160, H256},
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
use arrayvec::ArrayVec;
use dashmap::DashMap;
use hashbrown::HashMap;
use hashlink::LruCache;
use parking_lot::Mutex;
use rand::Rng;
use std::{sync::Arc, time::Duration};
use task_group::TaskGroup;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tracing::*;

pub const PER_SENDER: usize = 32;
pub type TransactionList<T = Transaction> = ArrayVec<T, PER_SENDER>;

pub struct SharedState {}

pub struct Pool {
    pub node: Arc<Node>,

    pub by_hash: DashMap<H256, Transaction>,
    pub by_sender: HashMap<H160, TransactionList>,

    pub unprocessed: Mutex<LruCache<Transaction, ()>>,

    pub worst_queue: (),
    pub best_queue: (),
}

impl Pool {
    pub fn builder() -> PoolBuilder {
        PoolBuilder::default()
    }

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
                    let transactions = hashes
                        .iter()
                        .filter_map(|hash| {
                            this.by_hash
                                .get(hash)
                                .map(|shared_ref| shared_ref.clone().msg)
                        })
                        .collect::<Vec<_>>();
                    this.node
                        .send_pooled_transactions(
                            request_id,
                            transactions,
                            PeerFilter::Peer(peer_id, sentry_id),
                        )
                        .await;
                }
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
                        Message::Transactions(Transactions(transactions)) => {
                            match transactions
                                .into_iter()
                                .map(|msg| Transaction::try_from(msg).map(|tx| (tx, ())))
                                .collect::<Result<Vec<_>, _>>()
                            {
                                Ok(transactions) => {
                                    this.unprocessed.lock().extend(transactions);
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
                                .map(|msg| Transaction::try_from(msg).map(|tx| (tx, ())))
                                .collect::<Result<Vec<_>, _>>()
                            {
                                Ok(transactions) => {
                                    this.unprocessed.lock().extend(transactions);
                                }
                                Err(_) => {
                                    penalty_tx
                                        .send((peer_id, PenaltyKind::MalformedTransaction))
                                        .await?;
                                }
                            }
                        }
                        _ => unreachable!(),
                    }
                }

                Ok::<_, anyhow::Error>(())
            }
        });

        tasks.spawn({
            let _this = self.node.clone();

            async move {}
        });

        std::future::pending::<()>().await;
    }
}
