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
        let _node = self
            .node
            .ok_or_else(|| anyhow::anyhow!("node is required"))?;

        todo!()
    }
}
