pub mod models;
mod sqlx_client;

use crate::models::{BufferedConsumerChannels, BufferedConsumerConfig, RawTransaction};
use crate::sqlx_client::{
    create_table_raw_transactions, get_count_raw_transactions, get_raw_transactions,
    new_raw_transaction,
};
use chrono::Utc;
use futures::channel::mpsc;
use futures::channel::mpsc::{UnboundedReceiver, UnboundedSender};
use futures::SinkExt;
use futures::StreamExt;
use indexer_lib::{
    AnyExtractable, AnyExtractableOutput, ExtractInput, ParsedOutput, TransactionExt,
};
use sqlx::PgPool;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::sleep;
use ton_block::{GetRepresentationHash, Transaction};
use ton_types::UInt256;

#[allow(clippy::type_complexity)]
pub fn start_parsing_and_get_channels(
    config: BufferedConsumerConfig
) -> BufferedConsumerChannels {
    let (tx_parsed_events, rx_parsed_events) = mpsc::unbounded();
    let (tx_commit, rx_commit) = mpsc::unbounded();
    let notify_for_services = Arc::new(Notify::new());

    {
        let notify_for_services = notify_for_services.clone();
        tokio::spawn(parse_kafka_transactions(
            config,
            tx_parsed_events,
            notify_for_services,
            rx_commit,
        ));
    }
    BufferedConsumerChannels {
        rx_parsed_events,
        tx_commit,
        notify_for_services,
    }
}

#[allow(clippy::too_many_arguments)]
async fn parse_kafka_transactions(
    config: BufferedConsumerConfig,
    tx_parsed_events: UnboundedSender<Vec<(ParsedOutput<AnyExtractableOutput>, RawTransaction)>>,
    notify_for_services: Arc<Notify>,
    commit_rx: UnboundedReceiver<()>,
) {
    create_table_raw_transactions(&config.pg_pool).await;

    let reset = get_count_raw_transactions(&config.pg_pool).await == 0;

    let mut stream_transactions = config.transaction_consumer
        .stream_transactions(reset)
        .await
        .expect("cant get stream transactions");

    let mut i: i64 = 0;

    while let Some(produced_transaction) = stream_transactions.next().await {
        let transaction: Transaction = produced_transaction.transaction.clone();
        let transaction_time = transaction.time() as i32;
        if extract_events(&transaction, transaction.hash().unwrap(), &config.events_to_parse).is_some() {
            new_raw_transaction(transaction.into(), &config.pg_pool)
                .await
                .expect("cant insert raw_transaction to db");
        }
        if transaction_time - config.timestamp_sync > 0 {
            produced_transaction.commit().unwrap();
            let pg_pool = config.pg_pool.clone();
            let events = config.events_to_parse.clone();
            tokio::spawn(parse_raw_transaction(
                events,
                tx_parsed_events,
                notify_for_services,
                config.timestamp_sync,
                pg_pool,
                config.delay,
                commit_rx,
            ));
            break;
        }
        if i % 10_000 == 0 {
            produced_transaction.commit().unwrap();
            log::info!("COMMIT KAFKA 10_000");
            i = 0;
        }
        i += 1;
    }

    while let Some(produced_transaction) = stream_transactions.next().await {
        let transaction: Transaction = produced_transaction.transaction.clone();
        let transaction_timestamp = transaction.now;
        if extract_events(&transaction, transaction.hash().unwrap(), &config.events_to_parse).is_some() {
            new_raw_transaction(transaction.into(), &config.pg_pool)
                .await
                .expect("cant insert raw_transaction to db");
        }
        if i % 10_000 == 0 {
            produced_transaction.commit().unwrap();
            log::info!(
                "COMMIT KAFKA 10_000 timestamp_block {}",
                transaction_timestamp
            );
            i = 0;
        }
        i += 1;
    }
}

async fn parse_raw_transaction(
    events: Vec<AnyExtractable>,
    mut tx: UnboundedSender<Vec<(ParsedOutput<AnyExtractableOutput>, RawTransaction)>>,
    notify: Arc<Notify>,
    timestamp_sync: i32,
    pg_pool: PgPool,
    secs_delay: i32,
    mut commit_rx: UnboundedReceiver<()>,
) {
    let mut notified = false;
    let count_all_raw = get_count_raw_transactions(&pg_pool).await;
    let mut i: i64 = 0;

    loop {
        let timestamp_now = Utc::now().timestamp() as i32;

        let mut begin = pg_pool
            .begin()
            .await
            .expect("cant get pg transaction");

        let raw_transactions_from_db =
            get_raw_transactions(5000, timestamp_now - secs_delay, &mut begin)
                .await
                .unwrap_or_default();

        if raw_transactions_from_db.is_empty() {
            if !notified {
                notify.notify_one();
                notified = true;
            }
            sleep(Duration::from_secs(1)).await;
            continue;
        }

        let mut send_message = vec![];
        for raw_transaction_from_db in raw_transactions_from_db {
            i += 1;
            if !notified && i % 5000 == 0 {
                log::info!("parsing {}/{}", i, count_all_raw);
            }

            if !notified && raw_transaction_from_db.timestamp_block > timestamp_sync {
                notify.notify_one();
                notified = true;
            }

            let raw_transaction = match RawTransaction::try_from(raw_transaction_from_db) {
                Ok(ok) => ok,
                Err(e) => {
                    log::error!("{}", e);
                    continue;
                }
            };

            if let Some(events) =
                extract_events(&raw_transaction.data, raw_transaction.hash, &events)
            {
                send_message.push((events, raw_transaction));
            };
        }
        tx.send(send_message).await.expect("dead sender");
        commit_rx.next().await;
        begin.commit().await.expect("cant commit db update");
    }
}

pub fn extract_events(
    data: &Transaction,
    hash: UInt256,
    events: &[AnyExtractable],
) -> Option<ParsedOutput<AnyExtractableOutput>> {
    ExtractInput {
        transaction: data,
        what_to_extract: events,
        hash,
    }
    .process()
    .ok()
    .flatten()
}

// struct BufferedTransactionConsumer {
//     tx_consumer: TransactionConsumer,
//     sqlx_client: PgPool,
//     config: Config,
//     notify: Arc<Notify>,
// }
//
// struct Config {
//     delay: Duration,
//     sync_timestamp: i32,
// }
//
// impl BufferedTransactionConsumer {
//     pub fn new(sqlx_client: PgPool, config: Config, consumer: TransactionConsumer, notify: Arc<Notify>) -> Arc<Self> {
//         Arc::new(Self {
//             tx_consumer: consumer,
//             sqlx_client,
//             config,
//             notify
//         })
//     }
//
//     pub fn spawn_listener(self: &Arc<Self>) -> ListenerHandle {
//         let (tx, rx) = mpsc::channel(128);
//     }
// }
//
// struct ListenerHandle {}
//
// impl ListenerHandle {
//     pub fn stream(&self) -> UnboundedReceiver<RawTransaction> {
//         unimplemented!()
//     }
// }
