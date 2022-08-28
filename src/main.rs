use crate::Error::UnexpectedError;
use csv::{ByteRecord, Reader, Trim};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::{env, io};
use thiserror::Error as ThisError;

#[derive(Debug, Deserialize)]
struct TransactionEntry<'a> {
    #[serde(rename = "type")]
    tx_type: &'a [u8],
    client: u16,
    tx: u32,
    amount: Option<Decimal>,
}

#[derive(Debug, Clone, Serialize)]
struct ClientInfo {
    available: Decimal,
    held: Decimal,
    total: Decimal,
    locked: bool,
}

// create an enum for the different dispute stages
#[derive(PartialEq, Serialize)]
enum DisputeStage {
    None,
    Open,
    ChargeBack,
}

// create a struct called transaction
#[derive(Serialize)]
struct Transaction {
    client: u16,
    amount: Decimal,
    dispute_stage: DisputeStage,
}

#[derive(Debug, ThisError)]
enum Error {
    #[error("Error reading transaction file: {0:?}")]
    ReadError(#[from] io::Error),
    #[error("Error parsing transaction file: {0:?}")]
    ParseError(#[from] csv::Error),
    #[error("Unexpected error while processing the transaction: {0:?}")]
    UnexpectedError(String),
}

fn process_transactions<R>(
    mut rdr: Reader<R>,
    mut raw_record: ByteRecord,
    client_info: &mut HashMap<u16, ClientInfo>,
    headers: ByteRecord,
) -> Result<(), Error>
where
    R: io::Read,
{
    let mut tx_map: HashMap<u32, Transaction> = HashMap::new();
    while rdr.read_byte_record(&mut raw_record)? {
        let record: TransactionEntry = raw_record.deserialize(Some(&headers))?;

        // if the client is locked, continue
        if client_info.contains_key(&record.client) {
            match client_info.get(&record.client) {
                Some(client) => {
                    if client.locked {
                        continue;
                    }
                }
                None => {
                    return Err(UnexpectedError(format!(
                        "Client id {} not found",
                        record.client
                    )))
                }
            };
        }

        match record.tx_type {
            b"deposit" => {
                if tx_map.contains_key(&record.tx) {
                    continue;
                }

                // if record.amount is None, continue
                if record.amount.is_none() {
                    continue;
                }

                let amount_option: Option<Decimal> = record.amount.map(|amt: Decimal| {
                    let client_funds = client_info.entry(record.client).or_insert(ClientInfo {
                        available: dec!(0.0),
                        held: dec!(0.0),
                        total: dec!(0.0),
                        locked: false,
                    });
                    client_funds.available += amt;
                    client_funds.total += amt;
                    amt
                });

                let amount = match amount_option {
                    Some(amt) => amt,
                    None => continue, // partner side error, ignore and continue to next transaction
                };

                tx_map.insert(
                    record.tx,
                    Transaction {
                        client: record.client,
                        amount,
                        dispute_stage: DisputeStage::None,
                    },
                );
            }
            b"withdrawal" => {
                // if amount is none or if the client id is something that have not been seen before, continue to next transaction
                if record.amount.is_none() || !client_info.contains_key(&record.client) {
                    continue;
                }

                let client_funds = match client_info.get_mut(&record.client) {
                    Some(funds) => funds,
                    None => {
                        return Err(Error::UnexpectedError(format!(
                            "Client id {} not found",
                            record.client
                        )))
                    }
                };

                let amount_option: Option<Decimal> = record.amount.map(|amt| {
                    // if there are enough available funds to withdraw, withdraw the amount
                    if client_funds.available >= amt {
                        client_funds.available -= amt;
                        client_funds.total -= amt;
                        amt
                    } else {
                        dec!(-1.0)
                    }
                });

                let amount = match amount_option {
                    Some(amt) => amt,
                    None => continue, // partner side error, ignore and continue to next transaction
                };

                if amount >= dec!(0.0) {
                    tx_map.insert(
                        record.tx,
                        Transaction {
                            client: record.client,
                            amount,
                            dispute_stage: DisputeStage::None,
                        },
                    );
                }
            }
            b"dispute" => {
                if !tx_map.contains_key(&record.tx) || !client_info.contains_key(&record.client) {
                    continue;
                }

                let tx = match tx_map.get_mut(&record.tx) {
                    Some(tx) => tx,
                    None => {
                        return Err(Error::UnexpectedError(format!(
                            "Transaction id {} not found",
                            record.tx
                        )))
                    }
                };

                // if the client in tx does not match the client in the dispute or if dispute stage is not None, continue
                if tx.client != record.client || tx.dispute_stage != DisputeStage::None {
                    continue;
                }

                tx.dispute_stage = DisputeStage::Open;

                let client_funds = match client_info.get_mut(&record.client) {
                    Some(funds) => funds,
                    None => continue, // partner side error, ignore and continue to next transaction
                };

                // decrease the available funds by the amount in the tx
                client_funds.available -= tx.amount;
                client_funds.held += tx.amount;
            }
            b"resolve" => {
                if !tx_map.contains_key(&record.tx) || !client_info.contains_key(&record.client) {
                    continue;
                }

                let tx = match tx_map.get_mut(&record.tx) {
                    Some(tx) => tx,
                    None => {
                        return Err(Error::UnexpectedError(format!(
                            "Transaction id {} not found",
                            record.tx
                        )))
                    }
                };

                // if the client in tx does not match the client in the dispute, continue
                if tx.client != record.client || tx.dispute_stage != DisputeStage::Open {
                    continue;
                }

                let client_funds = match client_info.get_mut(&record.client) {
                    Some(funds) => funds,
                    None => continue, // partner side error, ignore and continue to next transaction
                };

                client_funds.available += tx.amount;
                client_funds.held -= tx.amount;
            }
            b"chargeback" => {
                if !tx_map.contains_key(&record.tx) || !client_info.contains_key(&record.client) {
                    continue;
                }

                let tx = match tx_map.get_mut(&record.tx) {
                    Some(tx) => tx,
                    None => {
                        return Err(Error::UnexpectedError(format!(
                            "Transaction id {} not found",
                            record.tx
                        )))
                    }
                };

                if tx.client != record.client || tx.dispute_stage != DisputeStage::Open {
                    continue;
                }

                let client_funds = match client_info.get_mut(&record.client) {
                    Some(funds) => funds,
                    None => continue, // partner side error, ignore and continue to next transaction
                };

                client_funds.total -= tx.amount;
                client_funds.held -= tx.amount;
                tx.dispute_stage = DisputeStage::ChargeBack;

                // lock the clients account
                client_funds.locked = true;
            }
            _ => {
                continue; // partner side error, ignore and continue to next transaction
            }
        }
    }
    Ok(())
}

fn process_transactions_from_path(path: String) -> Result<(), Error> {
    // create a reader for the csv file
    let mut rdr = csv::ReaderBuilder::new().
        trim(Trim::All).
        flexible(true). // to allow amount to be skipped in case of disputes, resolutions and chargebacks
        from_path(path)?;

    // Reading into a ByteRecord instead of a StringRecord for best performance
    let raw_record = csv::ByteRecord::new();
    let headers = rdr.byte_headers()?.clone();

    let mut client_info: HashMap<u16, ClientInfo> = HashMap::new();

    process_transactions(rdr, raw_record, &mut client_info, headers)?;
    write_client_info(&client_info)?;
    Ok(())
}

fn write_client_info(client_info: &HashMap<u16, ClientInfo>) -> Result<(), Error> {
    let mut wtr = csv::Writer::from_writer(io::stdout());
    // write headers
    wtr.write_record(&["client", "available", "held", "total", "locked"])?;
    for (client, info) in client_info.iter() {
        wtr.serialize((
            client,
            &info.available,
            &info.held,
            &info.total,
            &info.locked,
        ))?;
    }

    // flush the writer
    wtr.flush()?;
    Ok(())
}

fn main() {
    let args: Vec<String> = env::args().collect();
    // assert that there is only one argument provided
    assert_eq!(args.len(), 2);
    let file_path = args[1].clone();
    match process_transactions_from_path(file_path) {
        Ok(_) => {}
        Err(e) => {
            println!("Error processing transactions: {:?}", e);
        }
    };
}
