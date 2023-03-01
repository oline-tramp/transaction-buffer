mod cache;
pub mod drop_base;
pub mod models;
mod sqlx_client;

use crate::cache::RawCache;
use crate::models::{BufferedConsumerChannels, BufferedConsumerConfig, RawTransaction};
use crate::sqlx_client::{
    create_table_raw_transactions, get_count_not_processed_raw_transactions,
    get_count_raw_transactions, get_raw_transactions, insert_raw_transaction,
    insert_raw_transactions, update_raw_transactions_set_processed_true,
};
use chrono::NaiveDateTime;
use futures::channel::mpsc::{Receiver, Sender};
use futures::SinkExt;
use futures::StreamExt;
use indexer_lib::AnyExtractableOutput::{Event, Function};
use indexer_lib::{
    AnyExtractable, AnyExtractableOutput, ParsedEvent, ParsedFunction, ParsedMessage, ParsedOutput,
    TransactionExt,
};
use nekoton_abi::transaction_parser::{Extracted, ParsedType};
use nekoton_abi::TransactionParser;
use sqlx::PgPool;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Notify, RwLock};
use tokio::time::sleep;
use ton_block::{GetRepresentationHash, TrComputePhase, Transaction};
use ton_types::UInt256;
use transaction_consumer::StreamFrom;

#[allow(clippy::type_complexity)]
pub fn start_parsing_and_get_channels(config: BufferedConsumerConfig) -> BufferedConsumerChannels {
    let (tx_parsed_events, rx_parsed_events) = futures::channel::mpsc::channel(1);
    let (tx_commit, rx_commit) = futures::channel::mpsc::channel(1);
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

pub fn test_from_raw_transactions(
    pg_pool: PgPool,
    events_to_parse: Vec<AnyExtractable>,
) -> BufferedConsumerChannels {
    let (tx_parsed_events, rx_parsed_events) = futures::channel::mpsc::channel(1);
    let (tx_commit, rx_commit) = futures::channel::mpsc::channel(1);
    let notify_for_services = Arc::new(Notify::new());
    let (functions, events) = from_any_extractable_to_functions_events(events_to_parse);

    let parser = TransactionParser::builder()
        .function_in_list(functions.clone(), false)
        .functions_out_list(functions, false)
        .events_list(events)
        .build()
        .unwrap();

    let timestamp_last_block = Arc::new(RwLock::new(i32::MAX));
    let time = Arc::new(RwLock::new(0));
    {
        let time = time.clone();
        tokio::spawn(timer(time));
    }
    tokio::spawn(parse_raw_transaction(
        parser,
        tx_parsed_events,
        notify_for_services.clone(),
        pg_pool,
        rx_commit,
        RawCache::new(),
        timestamp_last_block,
        time,
        2,
    ));
    BufferedConsumerChannels {
        rx_parsed_events,
        tx_commit,
        notify_for_services,
    }
}

async fn timer(time: Arc<RwLock<i32>>) {
    loop {
        *time.write().await += 1;
        sleep(Duration::from_secs(1)).await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn parse_kafka_transactions(
    config: BufferedConsumerConfig,
    tx_parsed_events: Sender<Vec<(ParsedOutput<AnyExtractableOutput>, RawTransaction)>>,
    notify_for_services: Arc<Notify>,
    commit_rx: Receiver<()>,
) {
    create_table_raw_transactions(&config.pg_pool).await;
    let (functions, events) = from_any_extractable_to_functions_events(config.events_to_parse);

    let parser = TransactionParser::builder()
        .function_in_list(functions.clone(), false)
        .functions_out_list(functions, false)
        .events_list(events)
        .build()
        .unwrap();

    let raw_cache = RawCache::new();

    let time = Arc::new(RwLock::new(0));
    {
        let time = time.clone();
        tokio::spawn(timer(time));
    }

    let stream_from = match get_count_raw_transactions(&config.pg_pool).await == 0 {
        true => StreamFrom::Beginning,
        false => StreamFrom::Stored,
    };

    let (mut stream_transactions, offsets) = config
        .transaction_consumer
        .stream_until_highest_offsets(stream_from)
        .await
        .expect("cant get highest offsets stream transactions");

    let timestamp_last_block = Arc::new(RwLock::new(0_i32));

    let mut count = 0;
    let mut raw_transactions = vec![];
    while let Some(produced_transaction) = stream_transactions.next().await {
        count += 1;
        let transaction: Transaction = produced_transaction.transaction.clone();
        let transaction_time = transaction.time() as i64;
        *timestamp_last_block.write().await = transaction_time as i32;

        if buff_extract_events(&transaction, transaction.hash().unwrap(), &parser).is_some() {
            raw_transactions.push(transaction.into());
        }

        if count >= config.buff_size || *time.read().await >= config.commit_time_secs {
            if !raw_transactions.is_empty() {
                insert_raw_transactions(&mut raw_transactions, &config.pg_pool)
                    .await
                    .expect("cant insert raw_transactions: rip db");
            }

            if let Err(e) = produced_transaction.commit() {
                log::error!("cant commit kafka, stream is down. ERROR {}", e);
            }

            log::info!(
                "COMMIT KAFKA {} transactions timestamp_block {} date: {}",
                count,
                transaction_time,
                NaiveDateTime::from_timestamp_opt(transaction_time, 0).unwrap()
            );
            count = 0;
            *time.write().await = 0;
        }
    }

    if !raw_transactions.is_empty() {
        insert_raw_transactions(&mut raw_transactions, &config.pg_pool)
            .await
            .expect("cant insert raw_transaction: rip db");
    }

    log::info!("kafka synced");

    {
        let pg_pool = config.pg_pool.clone();
        let parser = parser.clone();
        let raw_cache = raw_cache.clone();
        let timestamp_last_block = timestamp_last_block.clone();
        let timer = time.clone();
        tokio::spawn(parse_raw_transaction(
            parser,
            tx_parsed_events,
            notify_for_services,
            pg_pool,
            commit_rx,
            raw_cache,
            timestamp_last_block,
            timer,
            config.cache_timer,
        ));
    }

    let mut i = 0;
    let mut stream_transactions = config
        .transaction_consumer
        .stream_transactions(StreamFrom::Offsets(offsets))
        .await
        .expect("cant get stream transactions");

    while let Some(produced_transaction) = stream_transactions.next().await {
        i += 1;
        let transaction: Transaction = produced_transaction.transaction.clone();
        let transaction_timestamp = transaction.now;

        if buff_extract_events(&transaction, transaction.hash().unwrap(), &parser).is_some() {
            insert_raw_transaction(transaction.clone().into(), &config.pg_pool)
                .await
                .expect("cant insert raw_transaction to db");
            raw_cache.insert_raw(transaction.into()).await;
        }

        *timestamp_last_block.write().await = transaction_timestamp as i32;
        *time.write().await = 0;

        produced_transaction.commit().expect("dead stream kafka");

        if i >= 5_000 {
            log::info!(
                "KAFKA 5_000 transactions timestamp_block {} date: {}",
                transaction_timestamp,
                NaiveDateTime::from_timestamp_opt(transaction_timestamp as i64, 0).unwrap()
            );
            i = 0;
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn parse_raw_transaction(
    parser: TransactionParser,
    mut tx: Sender<Vec<(ParsedOutput<AnyExtractableOutput>, RawTransaction)>>,
    notify: Arc<Notify>,
    pg_pool: PgPool,
    mut commit_rx: Receiver<()>,
    raw_cache: RawCache,
    timestamp_last_block: Arc<RwLock<i32>>,
    timer: Arc<RwLock<i32>>,
    cache_timer: i32,
) {
    let count_not_processed = get_count_not_processed_raw_transactions(&pg_pool).await;
    let mut i: i64 = 0;

    loop {
        let mut begin = pg_pool.begin().await.expect("cant get pg transaction");

        let raw_transactions_from_db =
            get_raw_transactions(1000, *timestamp_last_block.read().await, &mut begin)
                .await
                .unwrap_or_default();

        if raw_transactions_from_db.is_empty() {
            notify.notify_one();
            break;
        }

        let mut send_message = vec![];
        for raw_transaction_from_db in raw_transactions_from_db {
            i += 1;

            let raw_transaction = match RawTransaction::try_from(raw_transaction_from_db) {
                Ok(ok) => ok,
                Err(e) => {
                    log::error!("{}", e);
                    continue;
                }
            };

            if let Some(events) =
                buff_extract_events(&raw_transaction.data, raw_transaction.hash, &parser)
            {
                send_message.push((events, raw_transaction));
            };
        }
        if !send_message.is_empty() {
            tx.send(send_message).await.expect("dead sender");
            commit_rx.next().await;
        }
        begin.commit().await.expect("cant commit db update");

        if i >= count_not_processed {
            log::info!("end parse. synced");
            notify.notify_one();
            break;
        } else {
            log::info!("parsing {}/{}", i, count_not_processed);
        }
    }

    raw_cache.fill_raws(&pg_pool).await;

    loop {
        let (raw_transactions, times) =
            raw_cache.get_raws(*timestamp_last_block.read().await, timer.clone(), cache_timer).await;

        if raw_transactions.is_empty() {
            sleep(Duration::from_secs(1)).await;
            continue;
        }

        let mut send_message = vec![];
        for raw_transaction in raw_transactions {
            if let Some(events) =
                buff_extract_events(&raw_transaction.data, raw_transaction.hash, &parser)
            {
                send_message.push((events, raw_transaction));
            };
        }

        if !send_message.is_empty() {
            tx.send(send_message).await.expect("dead sender");
            commit_rx.next().await;
        }

        update_raw_transactions_set_processed_true(&pg_pool, times).await;
    }
}

pub fn from_any_extractable_to_functions_events(
    events: Vec<AnyExtractable>,
) -> (Vec<ton_abi::Function>, Vec<ton_abi::Event>) {
    let (mut funs, mut events) = events.into_iter().fold((vec![], vec![]), |mut res, x| {
        match x {
            AnyExtractable::Function(x) => res.0.push(x),
            AnyExtractable::Event(y) => res.1.push(y),
        };
        res
    });
    funs.sort_by_key(|x| x.get_function_id());
    funs.dedup_by(|x, y| x.get_function_id() == y.get_function_id());
    events.sort_by(|x, y| x.id.cmp(&y.id));
    events.dedup_by(|x, y| x.id == y.id);
    (funs, events)
}

pub fn extract_events(
    data: &Transaction,
    hash: UInt256,
    events: &[AnyExtractable],
) -> Option<ParsedOutput<AnyExtractableOutput>> {
    let (functions, events) = from_any_extractable_to_functions_events(events.to_vec());

    let parser = TransactionParser::builder()
        .function_in_list(functions.clone(), false)
        .functions_out_list(functions, false)
        .events_list(events)
        .build()
        .unwrap();

    if let Ok(extracted) = parser.parse(data) {
        if !extracted.is_empty() {
            return from_vec_extracted_to_any_extractable_output(extracted, hash, data.clone());
        }
    }

    None
}

pub fn buff_extract_events(
    data: &Transaction,
    hash: UInt256,
    parser: &TransactionParser,
) -> Option<ParsedOutput<AnyExtractableOutput>> {
    if let Ok(extracted) = parser.parse(data) {
        if !extracted.is_empty() {
            return from_vec_extracted_to_any_extractable_output(extracted, hash, data.clone());
        }
    }
    None
}

pub fn from_vec_extracted_to_any_extractable_output(
    extracted: Vec<Extracted>,
    tx_hash: UInt256,
    tx: Transaction,
) -> Option<ParsedOutput<AnyExtractableOutput>> {
    if extracted.is_empty() {
        return None;
    }

    if let Ok(true) = tx.read_description().map(|x| {
        if !x.is_aborted() {
            true
        } else {
            x.compute_phase_ref()
                .map(|x| match x {
                    TrComputePhase::Vm(x) => x.exit_code == 60,
                    TrComputePhase::Skipped(_) => false,
                })
                .unwrap_or(false)
        }
    }) {
    } else {
        return None;
    }

    let mut res = ParsedOutput {
        transaction: tx,
        hash: tx_hash,
        output: vec![],
    };

    for x in extracted {
        if x.parsed_type != ParsedType::Event && !x.is_in_message {
            continue;
        }

        let any_extractable = match x.parsed_type {
            ParsedType::FunctionInput => Function(ParsedFunction {
                function_name: x.name.to_string(),
                function_id: x.function_id,
                input: Some(ParsedMessage {
                    tokens: x.tokens,
                    hash: <[u8; 32]>::try_from(x.message.hash().unwrap().into_vec()).unwrap(),
                }),
                output: None,
            }),
            ParsedType::FunctionOutput => Function(ParsedFunction {
                function_name: x.name.to_string(),
                function_id: x.function_id,
                input: None,
                output: Some(ParsedMessage {
                    tokens: x.tokens,
                    hash: <[u8; 32]>::try_from(x.message.hash().unwrap().into_vec()).unwrap(),
                }),
            }),
            ParsedType::Event => Event(ParsedEvent {
                function_name: x.name.to_string(),
                event_id: x.function_id,
                input: Some(x.tokens).unwrap_or_default(),
                message_hash: <[u8; 32]>::try_from(x.message.hash().unwrap().into_vec()).unwrap(),
            }),
            ParsedType::BouncedFunction => continue,
        };
        res.output.push(any_extractable);
    }

    if res.output.is_empty() {
        return None;
    }

    Some(res)
}

#[cfg(test)]
mod test {
    use crate::{extract_events, from_vec_extracted_to_any_extractable_output};
    use nekoton_abi::TransactionParser;
    use ton_block::{Deserializable, GetRepresentationHash};

    #[test]
    fn test_empty() {
        let tx = "te6ccgECCQEAAgMAA7d3xF4oX2oWEwg5M5aR8xwgw6PQR7NiQn3ingdzt6NLnlAAAYzbaqqQGGge6YdqXzRGzEt95+94zU1ZfCjSJTBeSEvV1TdfJArAAAGM2xx3CCYpu36QADSAISBBqAUEAQIZBHtJDuaygBiAIH1iEQMCAG/Jh6EgTBRYQAAAAAAABAACAAAAAlv8RbySHn7Zsc6jPU5HH77NJiEltjMklhaKitL0Hn58QFAWDACeSFFMPQkAAAAAAAAAAAFJAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACCctRvCOj0IytqL1T55yj5A4CiFWjMA24f39CflxqMd3B4ruUkaHcmjwepyx+Zq9yebin0HUaTR+nX46leelD4ZXUCAeAIBgEB3wcAsWgA+IvFC+1CwmEHJnLSPmOEGHR6CPZsSE+8U8DudvRpc8sAOoJ7mWzKNw9aN8zgsFlHCBKE/DKTYv4bm/dZb5x149AQ6h5ywAYUWGAAADGbbVVSBMU3b9JAALloAdQT3MtmUbh60b5nBYLKOECUJ+GUmxfw3N+6y3zjrx6BAB8ReKF9qFhMIOTOWkfMcIMOj0EezYkJ94p4Hc7ejS55UO5rKAAGFFhgAAAxm2z5xITFN2/CP/urf0A=";
        let tx = ton_block::Transaction::construct_from_base64(&tx).unwrap();
        let abi = r#"{"ABI version":2,"version":"2.2","header":["pubkey","time"],"functions":[{"name":"constructor","inputs":[],"outputs":[]},{"name":"acceptUpgrade","inputs":[{"name":"code","type":"cell"},{"name":"newVersion","type":"uint8"}],"outputs":[]},{"name":"receiveTokenDecimals","inputs":[{"name":"decimals","type":"uint8"}],"outputs":[]},{"name":"setManager","inputs":[{"name":"_manager","type":"address"}],"outputs":[]},{"name":"removeToken","inputs":[{"name":"token","type":"address"}],"outputs":[]},{"name":"addToken","inputs":[{"name":"token","type":"address"}],"outputs":[]},{"name":"setCanon","inputs":[{"name":"token","type":"address"}],"outputs":[]},{"name":"enableToken","inputs":[{"name":"token","type":"address"}],"outputs":[]},{"name":"enableAll","inputs":[],"outputs":[]},{"name":"disableToken","inputs":[{"name":"token","type":"address"}],"outputs":[]},{"name":"disableAll","inputs":[],"outputs":[]},{"name":"getCanon","inputs":[{"name":"answerId","type":"uint32"}],"outputs":[{"name":"value0","type":"address"},{"components":[{"name":"decimals","type":"uint8"},{"name":"enabled","type":"bool"}],"name":"value1","type":"tuple"}]},{"name":"getTokens","inputs":[{"name":"answerId","type":"uint32"}],"outputs":[{"components":[{"name":"decimals","type":"uint8"},{"name":"enabled","type":"bool"}],"name":"_tokens","type":"map(address,tuple)"},{"name":"_canon","type":"address"}]},{"name":"onAcceptTokensBurn","inputs":[{"name":"_amount","type":"uint128"},{"name":"walletOwner","type":"address"},{"name":"value2","type":"address"},{"name":"remainingGasTo","type":"address"},{"name":"payload","type":"cell"}],"outputs":[]},{"name":"transferOwnership","inputs":[{"name":"newOwner","type":"address"}],"outputs":[]},{"name":"renounceOwnership","inputs":[],"outputs":[]},{"name":"owner","inputs":[],"outputs":[{"name":"owner","type":"address"}]},{"name":"_randomNonce","inputs":[],"outputs":[{"name":"_randomNonce","type":"uint256"}]},{"name":"version","inputs":[],"outputs":[{"name":"version","type":"uint8"}]},{"name":"manager","inputs":[],"outputs":[{"name":"manager","type":"address"}]}],"data":[{"key":1,"name":"_randomNonce","type":"uint256"},{"key":2,"name":"proxy","type":"address"}],"events":[{"name":"OwnershipTransferred","inputs":[{"name":"previousOwner","type":"address"},{"name":"newOwner","type":"address"}],"outputs":[]}],"fields":[{"name":"_pubkey","type":"uint256"},{"name":"_timestamp","type":"uint64"},{"name":"_constructorFlag","type":"bool"},{"name":"owner","type":"address"},{"name":"_randomNonce","type":"uint256"},{"name":"proxy","type":"address"},{"name":"version","type":"uint8"},{"components":[{"name":"decimals","type":"uint8"},{"name":"enabled","type":"bool"}],"name":"tokens","type":"map(address,tuple)"},{"name":"manager","type":"address"},{"name":"canon","type":"address"}]}"#;

        let abi = ton_abi::Contract::load(abi).unwrap();

        let funs = abi.functions.values().cloned().collect::<Vec<_>>();
        let parser = TransactionParser::builder()
            .function_in_list(&funs, false)
            .functions_out_list(&funs, false)
            .build()
            .unwrap();
        let test = parser.parse(&tx).unwrap();
        let test1 =
            from_vec_extracted_to_any_extractable_output(test, tx.hash().unwrap(), tx.clone())
                .unwrap();
        dbg!(test1);
    }
}
