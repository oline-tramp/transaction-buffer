use anyhow::Context;
use futures::channel::mpsc::{Receiver, Sender};
use indexer_lib::{AnyExtractable, AnyExtractableOutput, ParsedOutput};
use sqlx::PgPool;
use std::sync::Arc;
use tokio::sync::Notify;
use ton_block::{Deserializable, GetRepresentationHash, Serializable, Transaction};
use ton_types::UInt256;
use transaction_consumer::TransactionConsumer;

pub struct BufferedConsumerConfig {
    pub delay: i32,
    pub transaction_consumer: Arc<TransactionConsumer>,
    pub pg_pool: PgPool,
    pub events_to_parse: Vec<AnyExtractable>,
}

impl BufferedConsumerConfig {
    pub fn new(
        delay: i32,
        transaction_consumer: Arc<TransactionConsumer>,
        pg_pool: PgPool,
        events_to_parse: Vec<AnyExtractable>,
    ) -> Self {
        Self {
            delay,
            transaction_consumer,
            pg_pool,
            events_to_parse,
        }
    }
}

pub struct BufferedConsumerChannels {
    pub rx_parsed_events:
        Receiver<Vec<(ParsedOutput<AnyExtractableOutput>, RawTransaction)>>,
    pub tx_commit: Sender<()>,
    pub notify_for_services: Arc<Notify>,
}

#[derive(Debug, Clone)]
pub struct RawTransactionFromDb {
    pub transaction: Vec<u8>,
    pub transaction_hash: Vec<u8>,
    pub timestamp_block: i32,
    pub timestamp_lt: i64,
    pub created_at: i64,
    pub processed: bool,
}

#[derive(Clone, Debug)]
pub struct RawTransaction {
    pub hash: UInt256,
    pub data: Transaction,
}

impl TryFrom<RawTransactionFromDb> for RawTransaction {
    type Error = anyhow::Error;

    fn try_from(value: RawTransactionFromDb) -> std::result::Result<Self, Self::Error> {
        let transaction =
            ton_block::Transaction::construct_from_bytes(value.transaction.as_slice())?;

        Ok(RawTransaction {
            hash: UInt256::from_be_bytes(&value.transaction_hash),
            data: transaction,
        })
    }
}

impl From<Transaction> for RawTransactionFromDb {
    fn from(x: Transaction) -> Self {
        RawTransactionFromDb {
            transaction: x
                .write_to_bytes()
                .context("Failed serializing tx to bytes")
                .unwrap(),
            transaction_hash: x.hash().unwrap().as_slice().to_vec(),
            timestamp_block: x.now as i32,
            timestamp_lt: x.lt as i64,
            created_at: 0,
            processed: false,
        }
    }
}
