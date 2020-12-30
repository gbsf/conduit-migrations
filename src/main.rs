// SPDX-FileCopyrightText: 2020 Gabriel Souza Franco
//
// SPDX-License-Identifier: MIT

use std::{convert::TryInto, path::Path};
use itertools::Itertools;

pub const COUNTER: &str = "c";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args_os().collect::<Vec<_>>();

    let path = if args.len() > 1 {
        Path::new(&args[1])
    } else {
        Path::new("db")
    };

    let db = sled::Config::default()
            .path(path)
            .cache_capacity(1024 * 1024 * 1024)
            .print_profile_on_drop(true)
            .open()?;
    let statekey_short = db.open_tree("statekey_short")?;
    let stateid_pduid = db.open_tree("stateid_pduid")?;
    let globals = db.open_tree("global")?;

    let mut num_records = 0usize;

    let mut batch = sled::Batch::default();
    for (k, v) in stateid_pduid.scan_prefix(b"").filter_map(|p| p.ok()) {
        let (hash, rest) = k.split_at(32);
        let sv = v.splitn(2, |&b| b == 0xFF).collect_vec();

        let new_v = if sv.len() == 2 && sv[0].len() + sv[1].len() + 1 != 8 {
            sv[1].into()
        } else {
            v
        };

        if rest.len() != 9 { // 0xFF + u64::to_be_bytes()
            let short = match statekey_short.get(&rest[1..])? {
                Some(short) => {
                    let s: [u8; 8] = short.to_vec().try_into().expect("");
                    u64::from_be_bytes(s)
                },
                None => {
                    let short = next_count(&globals)?;
                    statekey_short
                        .insert(&rest[1..], &short.to_be_bytes())?;
                    short
                }
            };
            let mut new_key = hash.to_vec();
            new_key.push(0xFF);
            new_key.extend_from_slice(&short.to_be_bytes());
            batch.insert(new_key.clone(), new_v.clone());
            batch.remove(k);

            num_records += 1;
            if num_records % 100 == 0 {
                println!("Fixed {} records", num_records);
            }
        }
    }

    println!("Commiting {} records", num_records);
    stateid_pduid.apply_batch(batch)?;
    Ok(())
}

pub fn increment(old: Option<&[u8]>) -> Option<Vec<u8>> {
    let number = match old.map(|bytes| bytes.try_into()) {
        Some(Ok(bytes)) => {
            let number = u64::from_be_bytes(bytes);
            number + 1
        }
        _ => 1, // Start at one. since 0 should return the first event in the db
    };

    Some(number.to_be_bytes().to_vec())
}

pub fn next_count(globals: &sled::Tree) -> Result<u64, Box<dyn std::error::Error>> {
    let count = globals.update_and_fetch(COUNTER, increment)?.expect("increment is always Some");
    let bytes: [u8; 8] = (*count).try_into()?;
    Ok(u64::from_be_bytes(bytes))
}
