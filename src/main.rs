use std::{env, io};
use std::error::Error;
use serde::{Deserialize, Serialize};
use csv::Trim;
use std::collections::HashMap;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

#[derive(Debug, Deserialize)]
struct TransactionEntry<'a> {
    #[serde(rename = "type")]
    tx_type: &'a [u8],
    client: u16,
    tx: u32,
    amount: Option<Decimal>,
}

#[derive(Debug, Serialize)]
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
    ChargeBack
}

// create a struct called transaction
#[derive(Serialize)]
struct Transaction {
    client: u16,
    amount: Decimal,
    dispute_stage: DisputeStage,
}


fn read_csv(file_path: String) -> Result<(), Box<dyn Error>> {
    // read the csv file
    let mut rdr = csv::ReaderBuilder::new().
        trim(Trim::All).
        flexible(true). // to allow amount to be skipped in case of disputes, resolutions and chargebacks
        from_path(file_path).unwrap(); // TODO: handle error and use from_reader instead

    let mut raw_record = csv::ByteRecord::new();
    let headers = rdr.byte_headers()?.clone();

    let mut client_info: HashMap<u16, ClientInfo> = HashMap::new();

    let mut tx_map: HashMap<u32, Transaction> = HashMap::new();

    while rdr.read_byte_record(&mut raw_record)? {
        println!("comes in here");
        let record: TransactionEntry = raw_record.deserialize(Some(&headers))?;
        // print record
        // println!("{:?}", record);
        // if the client is locked, continue
        if client_info.contains_key(&record.client) {
            let client = client_info.get(&record.client).unwrap();
            if client.locked {
                continue;
            }
        }

        match record.tx_type {
            b"deposit" => {
                // throw an error if tx_id is already in the map else add it to the map
                if tx_map.contains_key(&record.tx) {
                    panic!("tx_id already in map"); // TODO: change this to a better error
                }

                let amount = record.amount.map(|amt| {
                    let client_funds = client_info.entry(record.client).or_insert(ClientInfo {
                        available: dec!(0.0),
                        held: dec!(0.0),
                        total: dec!(0.0),
                        locked: false,
                    });
                    client_funds.available += amt;
                    client_funds.total += amt;
                    amt
                }).ok_or("amount is None")?; // TODO: return this error properly

                tx_map.insert(record.tx, Transaction {
                    client: record.client,
                    amount,
                    dispute_stage: DisputeStage::None,
                });
            },
            b"withdrawal" => {
                // throw an error if tx_id is already in the map else add it to the map
                if tx_map.contains_key(&record.tx) {
                    panic!("tx_id already in map"); // TODO: change this to a better error
                }

                if !client_info.contains_key(&record.client) {
                    continue;
                }
                let client_funds = client_info.get_mut(&record.client).expect("client should be in the map");

                // print client funds
                println!("{:?}", client_funds);
                let amount = record.amount.map(|amt| {
                    // if there are enough available funds to withdraw, withdraw the amount
                    if client_funds.available >= amt {
                        client_funds.available -= amt;
                        client_funds.total -= amt;
                        amt
                    } else {
                        dec!(-1.0)
                    }
                }).ok_or("amount is None")?; // TODO: return this error properly

                if amount >= dec!(0.0) {
                    tx_map.insert(record.tx, Transaction {
                        client: record.client,
                        amount,
                        dispute_stage: DisputeStage::None,
                    });
                }
            },
            b"dispute" => {
                if !tx_map.contains_key(&record.tx) || !client_info.contains_key(&record.client){
                    continue;
                }

                let tx = tx_map.get_mut(&record.tx).expect("tx should be in map"); // TOD0: change this to a better error

                // if the client in tx does not match the client in the dispute, continue
                if tx.client != record.client {
                    panic!("tx client does not match dispute client"); // TODO: change this to a better error
                }

                // dispute stage needs to be none
                if tx.dispute_stage != DisputeStage::None {
                    panic!("tx already in dispute"); // TODO: change this to a better error
                }

                tx.dispute_stage = DisputeStage::Open;

                // decrease the available funds by the amount in the tx
                let client_funds = client_info.get_mut(&record.client).unwrap();
                // print client funds
                println!("{:?}", client_funds);
                client_funds.available -= tx.amount;
                client_funds.held += tx.amount;
            },
            b"resolve" => {
                if !tx_map.contains_key(&record.tx) || !client_info.contains_key(&record.client){
                    continue;
                }

                let tx = tx_map.get_mut(&record.tx).expect("tx should be in map"); // TOD0: change this to a better error

                // if the client in tx does not match the client in the dispute, continue
                if tx.client != record.client || tx.dispute_stage != DisputeStage::Open{
                    continue
                }

                let client_funds = client_info.get_mut(&record.client).unwrap();
                client_funds.available += tx.amount;
                client_funds.held -= tx.amount;
            },
            b"chargeback" => {
                if !tx_map.contains_key(&record.tx) || !client_info.contains_key(&record.client){
                    continue;
                }
                let tx = tx_map.get_mut(&record.tx).expect("tx should be in map"); // TOD0: change this to a better error
                if tx.client != record.client || tx.dispute_stage != DisputeStage::Open{
                    continue
                }
                let client_funds = client_info.get_mut(&record.client).unwrap();
                // print client funds
                println!("{:?}", client_funds);
                client_funds.total -= tx.amount;
                client_funds.held -= tx.amount;
                tx.dispute_stage = DisputeStage::ChargeBack;

                // lock the clients account
                client_funds.locked = true;
            }
            _ => {
                println!("Unknown transaction type: {:?}", record.tx_type); // TODO: handle error instead of printing
            }
        }
    }

    let mut wtr = csv::Writer::from_writer(io::stdout());
    // write headers
    wtr.write_record(&["client","available","held","total","locked"])?;
    for (client, info) in client_info.iter() {
        // println!("{:?} and {:?}", client, info);
        wtr.serialize((client, &info.available, &info.held, &info.total, &info.locked))?;
    }

    // flush the writer
    wtr.flush()?;
    // println!("{:?}", client_info);
    Ok(())
}


fn main() {
    let args: Vec<String> = env::args().collect();
    // assert that there is only one argument provided
    assert_eq!(args.len(), 2);
    let file_path = args[1].clone();
    read_csv(file_path).unwrap();

    let number = dec!(-1.000);
    assert_eq!("-1.000", number.to_string());
    println!("{:?}", number);
}
