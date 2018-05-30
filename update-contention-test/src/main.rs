extern crate histogram;
extern crate postgres;
extern crate rand;
extern crate uuid;

use std::env;
use std::process;
use std::thread;
use std::time::{Duration, Instant};

use histogram::Histogram;
use postgres::{Connection, TlsMode};
use rand::distributions::{Distribution, Uniform};
use uuid::Uuid;

/*
 * This program assumes the existence of two postgresql tables: person
 * and person_count. The tables can be create with the following SQL statements:
 *     CREATE TABLE person (
 *       id serial PRIMARY KEY,
 *       otherid uuid,
 *       name text NOT NULL,
 *       data text
 *     );
 *
 *     CREATE INDEX person_data_idx ON person USING btree (data);
 *     CREATE INDEX person_name_idx ON person USING btree (name);
 *     CREATE UNIQUE INDEX person_otherid_idx ON person USING btree (otherid);
 *
 *     CREATE TABLE person_count (
 *       name text PRIMARY KEY,
 *       count bigint NOT NULL,
 *       bucket integer
 *     );
 */

const THREAD_COUNT: i32 = 16;
const THREAD_WRITES: i32 = 1000;
const BUCKET_COUNT: i32 = 100;

struct Person {
    _id: i32,
    otherid: Uuid,
    name: String,
    data: Option<String>,
}

fn run_single_cell_update_threads(thread_count: &i32) {
    let mut handles = Vec::new();
    for _number in 1..*thread_count {
        let h = thread::spawn(move || single_cell_updates());
        handles.push(h);
    }

    let mut read_histogram = Histogram::new();
    let mut write_histogram = Histogram::new();

    for handle in handles {
        match handle.join() {
            Ok((thread_read_hist, thread_write_hist)) => {
                read_histogram.merge(&thread_read_hist);
                write_histogram.merge(&thread_write_hist);
            }
            Err(_) => println!("single cell update thread panicked"),
        }
    }

    println!(
        "Read Latency Percentiles: p50: {} ns p90: {} ns p99: {} ns p999: {}",
        read_histogram.percentile(50.0).unwrap(),
        read_histogram.percentile(90.0).unwrap(),
        read_histogram.percentile(99.0).unwrap(),
        read_histogram.percentile(99.9).unwrap(),
    );

    println!(
        "Write Latency Percentiles: p50: {} ns p90: {} ns p99: {} ns p999: {}",
        write_histogram.percentile(50.0).unwrap(),
        write_histogram.percentile(90.0).unwrap(),
        write_histogram.percentile(99.0).unwrap(),
        write_histogram.percentile(99.9).unwrap(),
    );
}

fn single_cell_updates() -> (histogram::Histogram, histogram::Histogram) {
    let args: Vec<String> = env::args().collect();
    let url = &args[1];
    let thread_writes = if args.len() >= 4 {
        args[3].parse().unwrap_or(THREAD_WRITES)
    } else {
        THREAD_WRITES
    };

    let mut read_histogram = Histogram::new();
    let mut write_histogram = Histogram::new();
    let conn = Connection::connect(url.as_str(), TlsMode::None).unwrap();

    for _number in 1..thread_writes {
        let p = Person {
            _id: 0,
            otherid: Uuid::new_v4(),
            name: "Steven".to_string(),
            data: Some(("a").to_string().repeat(999)),
        };

        let read_start = Instant::now();
        let read_trans = conn.transaction().unwrap();

        read_trans
            .execute("SELECT count FROM person_count WHERE name = 'Steven'", &[])
            .unwrap();

        read_trans.commit().unwrap();
        let read_end = Instant::now();

        let read_duration = read_end.duration_since(read_start);
        let read_nanos =
            read_duration.as_secs() * 1_000_000_000 + read_duration.subsec_nanos() as u64;
        read_histogram.increment(read_nanos).unwrap();

        let write_start = Instant::now();
        let write_trans = conn.transaction().unwrap();

        write_trans
            .execute(
                "INSERT INTO person (otherid, name, data) VALUES ($1, $2, $3)",
                &[&p.otherid, &p.name, &p.data],
            )
            .unwrap();

        write_trans.execute("INSERT INTO person_count (name, count) VALUES ($1, 1) ON CONFLICT (name) DO UPDATE SET count = person_count.count + 1 WHERE person_count.name = $1",
                      &[&p.name]).unwrap();

        write_trans.commit().unwrap();
        let write_end = Instant::now();

        let write_duration = write_end.duration_since(write_start);
        let write_nanos =
            write_duration.as_secs() * 1_000_000_000 + write_duration.subsec_nanos() as u64;
        write_histogram.increment(write_nanos).unwrap();
    }

    (read_histogram, write_histogram)
}

fn delete_tables(conn: &Connection) {
    let trans = conn.transaction().unwrap();

    trans.execute("DELETE FROM person;", &[]).unwrap();
    trans.execute("DELETE FROM person_count;", &[]).unwrap();

    trans.commit().unwrap();
}

fn run_bucketed_update_threads(thread_count: &i32) {
    let mut handles = Vec::new();

    let mut read_histogram = Histogram::new();
    let mut write_histogram = Histogram::new();

    for _number in 1..*thread_count {
        let h = thread::spawn(move || bucketed_updates());
        handles.push(h);
    }

    for handle in handles {
        match handle.join() {
            Ok((thread_read_hist, thread_write_hist)) => {
                read_histogram.merge(&thread_read_hist);
                write_histogram.merge(&thread_write_hist);
            }
            Err(_) => println!("single cell update thread panicked"),
        }
    }

    println!(
        "Read Latency Percentiles: p50: {} ns p90: {} ns p99: {} ns p999: {}",
        read_histogram.percentile(50.0).unwrap(),
        read_histogram.percentile(90.0).unwrap(),
        read_histogram.percentile(99.0).unwrap(),
        read_histogram.percentile(99.9).unwrap(),
    );

    println!(
        "Write Latency Percentiles: p50: {} ns p90: {} ns p99: {} ns p999: {}",
        write_histogram.percentile(50.0).unwrap(),
        write_histogram.percentile(90.0).unwrap(),
        write_histogram.percentile(99.0).unwrap(),
        write_histogram.percentile(99.9).unwrap(),
    );
}

fn bucketed_updates() -> (histogram::Histogram, histogram::Histogram) {
    let args: Vec<String> = env::args().collect();
    let url = &args[1];
    let thread_writes = if args.len() >= 4 {
        args[3].parse().unwrap_or(THREAD_WRITES)
    } else {
        THREAD_WRITES
    };
    let bucket_count = if args.len() >= 5 {
        args[4].parse().unwrap_or(BUCKET_COUNT)
    } else {
        BUCKET_COUNT
    };

    let mut read_histogram = Histogram::new();
    let mut write_histogram = Histogram::new();
    let bucket_distribution = Uniform::from(1..bucket_count);
    let mut rng = rand::thread_rng();
    let conn = Connection::connect(url.as_str(), TlsMode::None).unwrap();

    for _number in 1..thread_writes {
        let p = Person {
            _id: 0,
            otherid: Uuid::new_v4(),
            name: "Steven".to_string(),
            data: Some(("a").to_string().repeat(999)),
        };

        let read_start = Instant::now();
        let read_trans = conn.transaction().unwrap();

        read_trans
            .execute(
                "SELECT sum(count) FROM person_count WHERE name = 'Steven'",
                &[],
            )
            .unwrap();

        read_trans.commit().unwrap();
        let read_end = Instant::now();

        let read_duration = read_end.duration_since(read_start);
        let read_nanos =
            read_duration.as_secs() * 1_000_000_000 + read_duration.subsec_nanos() as u64;
        read_histogram.increment(read_nanos).unwrap();

        let bucket = bucket_distribution.sample(&mut rng);
        let write_start = Instant::now();
        let write_trans = conn.transaction().unwrap();

        write_trans
            .execute(
                "INSERT INTO person (otherid, name, data) VALUES ($1, $2, $3)",
                &[&p.otherid, &p.name, &p.data],
            )
            .unwrap();

        write_trans.execute("INSERT INTO person_count (name, count, bucket) VALUES ($1, 1, $2) ON CONFLICT (name) DO UPDATE SET count = person_count.count + 1 WHERE person_count.name = $1 AND person_count.bucket = $2",
                      &[&p.name, &bucket]).unwrap();

        write_trans.commit().unwrap();
        let write_end = Instant::now();

        let write_duration = write_end.duration_since(write_start);
        let write_nanos =
            write_duration.as_secs() * 1_000_000_000 + write_duration.subsec_nanos() as u64;
        write_histogram.increment(write_nanos).unwrap();
    }

    (read_histogram, write_histogram)
}

fn usage() {
    println!("Usage: update-contention-test PG_URL [THREAD_COUNT][THREAD_WRITES] [BUCKET_COUNT]");
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
    run_single_cell_update_threads(&thread_count);
    let end1 = Instant::now();
    println!("Single cell updates: {:?}", end1.duration_since(start1));

    delete_tables(&conn);

    thread::sleep(Duration::from_secs(1));

    let start2 = Instant::now();
    run_bucketed_update_threads(&thread_count);
    let end2 = Instant::now();
    println!("Bucketed updates: {:?}", end2.duration_since(start2));

    delete_tables(&conn);
}
