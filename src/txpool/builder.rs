use hashlink::LruCache;
use parking_lot::Mutex;

use crate::{p2p::node::Node, txpool::Pool};
use std::sync::Arc;

#[derive(Default, Debug)]
pub struct PoolBuilder {
    node: Option<Arc<Node>>,
}

impl PoolBuilder {
    pub fn with_node(mut self, node: Arc<Node>) -> Self {
        self.node = Some(node);
        self
    }

    pub fn build(self) -> anyhow::Result<Pool> {
        let node = self
            .node
            .ok_or_else(|| anyhow::anyhow!("node is required"))?;

        Ok(Pool {
            node,
            by_hash: Default::default(),
            by_sender: Default::default(),
            unprocessed: Mutex::new(LruCache::new(1024)),
            worst_queue: (),
            best_queue: (),
        })
    }
}
