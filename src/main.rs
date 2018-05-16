extern crate postgres;
extern crate uuid;

use std::env;
use std::process;
use std::thread;
use std::time::{Duration,Instant};

use postgres::{Connection, TlsMode};
use uuid::Uuid;

// static URL: &'static str = "postgresql://kelly@localhost:5432/test";
const THREAD_COUNT: i32 = 16;
const THREAD_INSERTS: i32 = 10000;
const BATCH_SIZE: i32 = 100;

struct Person {
    _id: i32,
    otherid: Uuid,
    name: String,
    data: Option<String>,
}

fn run_separate_txn_threads(thread_count: &i32) {
    let mut handles = Vec::new();
    for _number in 1..*thread_count {
        let h = thread::spawn(move || separate_txns());
        handles.push(h);
    };

    for handle in handles {
        handle.join().unwrap();
    }
}

fn separate_txns() {
    let args: Vec<String> = env::args().collect();
    let url = &args[1];
    let thread_inserts = if args.len() >= 4 {
        args[3].parse().unwrap_or(THREAD_INSERTS)
    } else {
        THREAD_INSERTS
    };
    let conn = Connection::connect(url.as_str(), TlsMode::None).unwrap();

    for _number in 1..thread_inserts {
        let p = Person {
            _id: 0,
            otherid: Uuid::new_v4(),
            name: "Steven".to_string(),
            data: Some(("a").to_string().repeat(999))
        };

        let trans = conn.transaction().unwrap();

        trans.execute("INSERT INTO person (otherid, name, data) VALUES ($1, $2, $3)",
                      &[&p.otherid,&p.name, &p.data]).unwrap();

        trans.commit().unwrap();
    }
}

fn delete_table(conn: &Connection) {
    let trans = conn.transaction().unwrap();

    trans.execute("DELETE FROM person;", &[]).unwrap();

    trans.commit().unwrap();
}

fn run_batched_txn_threads(thread_count: &i32) {
    let mut handles = Vec::new();

    for _number in 1..*thread_count {
        let h = thread::spawn(move || {
           batched_txns();
        });
        handles.push(h);
    };

    for handle in handles {
        handle.join().unwrap();
    }
}

fn batched_txns() {
    let args: Vec<String> = env::args().collect();
    let url = &args[1];
    let thread_inserts = if args.len() >= 4 {
        args[3].parse().unwrap_or(THREAD_INSERTS)
    } else {
        THREAD_INSERTS
    };
    let batch_size = if args.len() >= 5 {
        args[4].parse().unwrap_or(BATCH_SIZE)
    } else {
        BATCH_SIZE
    };
    let conn = Connection::connect(url.as_str(), TlsMode::None).unwrap();

    let txn_count = thread_inserts / batch_size;

    for _txn_num in 1..txn_count {
        let trans = conn.transaction().unwrap();

        for _number in 1..batch_size {
            let p = Person {
                _id: 0,
                otherid: Uuid::new_v4(),
                name: "Steven".to_string(),
                data: Some(("a").to_string().repeat(999))
            };

            trans.execute("INSERT INTO person (otherid, name, data) VALUES ($1, $2, $3)",
                          &[&p.otherid, &p.name, &p.data]).unwrap();
        }

        trans.commit().unwrap();
    }

    // Perform any extra inserts if THREAD_INSERTS isn't evenly factored by
    // BATCH_SIZE
    let insert_remainder = thread_inserts % batch_size;
    if insert_remainder > 0 {
        let trans = conn.transaction().unwrap();

        for _number in 1..insert_remainder {
            let p = Person {
                _id: 0,
                otherid: Uuid::new_v4(),
                name: "Steven".to_string(),
                data: Some(("a").to_string().repeat(999))
            };

            trans.execute("INSERT INTO person (otherid, name, data) VALUES ($1, $2, $3)",
                          &[&p.otherid, &p.name, &p.data]).unwrap();
        }

        trans.commit().unwrap();
    }
}

fn usage() {
    println!("Usage: pg-test PG_URL [THREAD_COUNT] [THREAD_INSERTS] [BATCH_SIZE]");
}

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        usage();
        process::exit(1);
    }

    let url = &args[1];
    let thread_count = if args.len() >= 3 {
        args[2].parse().unwrap_or(THREAD_COUNT)
    } else {
        THREAD_COUNT
    };

    let conn = Connection::connect(url.as_str(), TlsMode::None).unwrap();

    let start1 = Instant::now();
    run_separate_txn_threads(&thread_count);
    let end1 = Instant::now();
    println!("Separate txns: {:?}", end1.duration_since(start1));

    delete_table(&conn);

    thread::sleep(Duration::from_secs(1));

    let start2 = Instant::now();
    run_batched_txn_threads(&thread_count);
    let end2 = Instant::now();
    println!("Batched txns: {:?}", end2.duration_since(start2));

    delete_table(&conn);
}
