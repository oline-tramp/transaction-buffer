use anyhow::Context;
use ton_block::{Deserializable, GetRepresentationHash, Serializable, Transaction};
use ton_types::UInt256;

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
