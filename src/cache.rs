use crate::models::RawTransactionFromDb;
use crate::RawTransaction;
use chrono::Utc;
use itertools::Itertools;
use sqlx::{Pool, Postgres};
use std::cmp::Ordering;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct RawCache(Arc<RwLock<Vec<RawTransaction>>>);

pub async fn get_raws() -> Vec<RawTransactionFromDb> {
    todo!()
}

impl RawCache {
    async fn new(pg_pool: &Pool<Postgres>) -> Self {
        let raw_transactions = get_raws()
            .await
            .into_iter()
            .map(RawTransaction::from)
            .collect_vec();
        Self(Arc::new(RwLock::new(raw_transactions)))
    }

    async fn insert_raw(&self, raw: RawTransaction) {
        self.0.write().await.push(raw);
    }

    async fn get_raws(&self, delay: i32) -> Vec<RawTransaction> {
        let timestamp_now = Utc::now().timestamp() as i32;
        let mut lock = self.0.write().await;

        let (res, cache) =
            lock.drain(..)
                .into_iter()
                .fold((vec![], vec![]), |(mut res, mut cache), x| {
                    if (x.data.now as i32) < (timestamp_now - delay) {
                        res.push(x)
                    } else {
                        cache.push(x)
                    };
                    (res, cache)
                });

        *lock = cache;

        res.into_iter()
            .sorted_by(|x, y| match x.data.now.cmp(&y.data.now) {
                Ordering::Less => Ordering::Less,
                Ordering::Equal => x.data.lt.cmp(&y.data.lt),
                Ordering::Greater => Ordering::Greater,
            })
            .collect_vec()
    }
}
