use crate::{
    kv::{
        mdbx::*,
        tables::{self, AccountChange, StorageChange, StorageChangeKey},
    },
    stagedsync::{stage::*, stages::*},
    stages::stage_util::*,
    StageId,
};
use async_trait::async_trait;

/// Generate account history index
#[derive(Debug)]
pub struct AccountHistoryIndex(pub IndexParams);

#[async_trait]
impl<'db, E> Stage<'db, E> for AccountHistoryIndex
where
    E: EnvironmentKind,
{
    fn id(&self) -> StageId {
        ACCOUNT_HISTORY_INDEX
    }

    async fn execute<'tx>(
        &mut self,
        tx: &'tx mut MdbxTransaction<'db, RW, E>,
        input: StageInput,
    ) -> Result<ExecOutput, StageError>
    where
        'db: 'tx,
    {
        Ok(execute_index(
            tx,
            input,
            &self.0,
            tables::AccountChangeSet,
            tables::AccountHistory,
            |block_number, AccountChange { address, .. }| (block_number, address),
        )?)
    }

    async fn unwind<'tx>(
        &mut self,
        tx: &'tx mut MdbxTransaction<'db, RW, E>,
        input: UnwindInput,
    ) -> anyhow::Result<UnwindOutput>
    where
        'db: 'tx,
    {
        unwind_index(
            tx,
            input,
            tables::AccountChangeSet,
            tables::AccountHistory,
            |_, AccountChange { address, .. }| address,
        )
    }

    async fn prune<'tx>(
        &mut self,
        tx: &'tx mut MdbxTransaction<'db, RW, E>,
        input: PruningInput,
    ) -> anyhow::Result<()>
    where
        'db: 'tx,
    {
        prune_index(
            tx,
            input,
            tables::AccountChangeSet,
            tables::AccountHistory,
            |block_number, AccountChange { address, .. }| (block_number, address),
        )
    }
}

/// Generate storage history index
#[derive(Debug)]
pub struct StorageHistoryIndex(pub IndexParams);

#[async_trait]
impl<'db, E> Stage<'db, E> for StorageHistoryIndex
where
    E: EnvironmentKind,
{
    fn id(&self) -> StageId {
        STORAGE_HISTORY_INDEX
    }

    async fn execute<'tx>(
        &mut self,
        tx: &'tx mut MdbxTransaction<'db, RW, E>,
        input: StageInput,
    ) -> Result<ExecOutput, StageError>
    where
        'db: 'tx,
    {
        Ok(execute_index(
            tx,
            input,
            &self.0,
            tables::StorageChangeSet,
            tables::StorageHistory,
            |StorageChangeKey {
                 block_number,
                 address,
             },
             StorageChange { location, .. }| (block_number, (address, location)),
        )?)
    }

    async fn unwind<'tx>(
        &mut self,
        tx: &'tx mut MdbxTransaction<'db, RW, E>,
        input: UnwindInput,
    ) -> anyhow::Result<UnwindOutput>
    where
        'db: 'tx,
    {
        unwind_index(
            tx,
            input,
            tables::StorageChangeSet,
            tables::StorageHistory,
            |StorageChangeKey { address, .. }, StorageChange { location, .. }| (address, location),
        )
    }

    async fn prune<'tx>(
        &mut self,
        tx: &'tx mut MdbxTransaction<'db, RW, E>,
        input: PruningInput,
    ) -> anyhow::Result<()>
    where
        'db: 'tx,
    {
        prune_index(
            tx,
            input,
            tables::StorageChangeSet,
            tables::StorageHistory,
            |StorageChangeKey {
                 block_number,
                 address,
             },
             StorageChange { location, .. }| (block_number, (address, location)),
        )
    }
}
