use crate::models::RawTransactionFromDb;
use anyhow::Result;
use sqlx::postgres::PgArguments;
use sqlx::{Arguments, PgPool};
use sqlx::{Postgres, Row, Transaction};
use std::cmp::Ordering;
use itertools::Itertools;

const INSERT_RAW_TRANSACTION_QUERY: &str = "INSERT INTO raw_transactions (transaction, transaction_hash, timestamp_block, timestamp_lt, processed) VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING";

const COUNT_RAW_TRANSACTION_QUERY: &str = "SELECT count(*) FROM raw_transactions";

const COUNT_RAW_NOT_PROCESSED_TRANSACTION_QUERY: &str = "SELECT count(*) FROM raw_transactions WHERE processed = false";

const GET_AND_UPDATE_RAW_TRANSACTIONS_QUERY: &str = "UPDATE raw_transactions
SET processed = true
WHERE (timestamp_block, timestamp_lt) IN (SELECT timestamp_block, timestamp_lt
                                          FROM raw_transactions
                                          WHERE timestamp_block < $1 AND processed = false
                                          ORDER BY (timestamp_block, timestamp_lt)
                                          LIMIT $2)
returning *;";

const CREATE_TABLE_RAW_TRANSACTIONS_QUERY: &str = "CREATE TABLE IF NOT EXISTS raw_transactions
(
    transaction      BYTEA   NOT NULL,
    transaction_hash BYTEA   NOT NULL,
    timestamp_block  INTEGER NOT NULL,
    timestamp_lt     BIGINT  NOT NULL,
    created_at       BIGINT  NOT NULL DEFAULT extract(epoch from (CURRENT_TIMESTAMP(3) at time zone 'utc')) * 1000,
    processed        BOOL    NOT NULL,
    PRIMARY KEY (transaction_hash)
);";

const CREATE_INDEX_RAW_TRANSACTIONS_QUERY: &str = "CREATE INDEX IF NOT EXISTS raw_transactions_ix_ttp ON raw_transactions (timestamp_block, timestamp_lt, processed);";

pub async fn new_raw_transaction(
    raw_transaction: RawTransactionFromDb,
    pg_pool: &PgPool,
) -> Result<()> {
    let mut args = PgArguments::default();
    args.add(raw_transaction.transaction);
    args.add(raw_transaction.transaction_hash);
    args.add(raw_transaction.timestamp_block);
    args.add(raw_transaction.timestamp_lt);
    args.add(raw_transaction.processed);

    sqlx::query_with(INSERT_RAW_TRANSACTION_QUERY, args)
        .execute(pg_pool)
        .await?;

    Ok(())
}

pub async fn get_raw_transactions(
    limit: i64,
    timestamp_block_lt: i32,
    begin: &mut Transaction<'_, Postgres>,
) -> Result<Vec<RawTransactionFromDb>> {
    let mut args = PgArguments::default();
    args.add(timestamp_block_lt);
    args.add(limit);

    sqlx::query_with(GET_AND_UPDATE_RAW_TRANSACTIONS_QUERY, args)
        .fetch_all(begin)
        .await
        .map(|y| {
            y.into_iter()
                .map(|x| RawTransactionFromDb {
                    transaction: x.get(0),
                    transaction_hash: x.get(1),
                    timestamp_block: x.get(2),
                    timestamp_lt: x.get(3),
                    created_at: x.get(4),
                    processed: x.get(5),
                })
                .sorted_by(|x, y| match x.timestamp_block.cmp(&y.timestamp_block) {
                    Ordering::Less => Ordering::Less,
                    Ordering::Equal => x.timestamp_lt.cmp(&y.timestamp_lt),
                    Ordering::Greater => Ordering::Greater,
                })
                .collect::<Vec<_>>()
        })
        .map_err(anyhow::Error::new)
}

pub async fn get_count_raw_transactions(pg_pool: &PgPool) -> i64 {
    let count: i64 = sqlx::query(COUNT_RAW_TRANSACTION_QUERY)
        .fetch_one(pg_pool)
        .await
        .map(|x| x.get(0))
        .unwrap_or_default();

    count
}

pub async fn get_count_not_processed_raw_transactions(pg_pool: &PgPool) -> i64 {
    let count: i64 = sqlx::query(COUNT_RAW_NOT_PROCESSED_TRANSACTION_QUERY)
        .fetch_one(pg_pool)
        .await
        .map(|x| x.get(0))
        .unwrap_or_default();

    count
}

pub async fn create_table_raw_transactions(pg_pool: &PgPool) {
    if let Err(e) = sqlx::query(CREATE_TABLE_RAW_TRANSACTIONS_QUERY)
        .execute(pg_pool)
        .await
    {
        log::error!("create table raw_transactions ERROR {}", e);
    }

    if let Err(e) = sqlx::query(CREATE_INDEX_RAW_TRANSACTIONS_QUERY)
        .execute(pg_pool)
        .await
    {
        log::error!("create index raw_transactions ERROR {}", e);
    }
}
