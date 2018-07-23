extern crate histogram;
extern crate postgres;
extern crate rand;
extern crate uuid;

use std::env;
use std::iter;
use std::process;
use std::thread;
use std::time::{Duration, Instant};

use histogram::Histogram;
use postgres::{Connection, TlsMode};
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use uuid::Uuid;

/*
 * This program was written to gauge performance of querying across many
 * postgres schemas versus a single public schema.
 *
 * The program assumes the existence of a manta_bucket table in the public
 * schema as well in each schema to be tested, name as manta_bucket_X where
 * X is from the set [1,10000] currently.
 *
 * The create-vnode-schemas.sh script can be used to facilitate creation of
 * the schemas.
 *
 *     ./create-vnode-schemas.sh {1..10000}
 */

const THREAD_COUNT: i32 = 16;
const THREAD_ITERATIONS: i32 = 1000;

struct MantaObject {
    key: String,
    bucket: String,
    owner: Uuid,
    vnode: i64,
    object_id: Uuid,
    content_length: i64,
    content_md5: String,
    content_type: String,
}

fn run_single_schema_threads(thread_count: &i32) {
    let mut handles = Vec::new();
    for _number in 1..*thread_count {
        let h = thread::spawn(move || single_schema_queries());
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

fn single_schema_queries() -> (histogram::Histogram, histogram::Histogram) {
    let args: Vec<String> = env::args().collect();
    let url = &args[1];
    let thread_iterations = args.get(3)
        .unwrap_or(&THREAD_ITERATIONS.to_string())
        .parse()
        .unwrap_or(THREAD_ITERATIONS);

    let mut read_histogram = Histogram::new();
    let mut write_histogram = Histogram::new();
    let conn = Connection::connect(url.as_str(), TlsMode::None).unwrap();

    let mut rng = thread_rng();

    for _number in 1..thread_iterations {
        let k: String = iter::repeat(())
            .map(|()| rng.sample(Alphanumeric))
            .take(10)
            .collect();
        let b: String = iter::repeat(())
            .map(|()| rng.sample(Alphanumeric))
            .take(12)
            .collect();
        let o = MantaObject {
            key: k,
            bucket: b,
            vnode: 1000,
            owner: Uuid::new_v4(),
            object_id: Uuid::new_v4(),
            content_length: 1024,
            content_md5: "deadbeef".to_string(),
            content_type: "text/plain".to_string(),
        };

        let write_start = Instant::now();
        let write_trans = conn.transaction().unwrap();
        let write_sql = "INSERT INTO manta_bucket (key, bucket, vnode, owner, \"objectId\", \"contentLength\", \"contentMD5\", \"contentType\") VALUES ($1, $2, $3, $4, $5, $6, $7, $8)";

        write_trans
            .execute(
                write_sql,
                &[
                    &o.key,
                    &o.bucket,
                    &o.vnode,
                    &o.owner,
                    &o.object_id,
                    &o.content_length,
                    &o.content_md5,
                    &o.content_type,
                ],
            )
            .unwrap();

        write_trans.commit().unwrap();
        let write_end = Instant::now();

        let write_duration = write_end.duration_since(write_start);
        let write_nanos =
            write_duration.as_secs() * 1_000_000_000 + write_duration.subsec_nanos() as u64;
        write_histogram.increment(write_nanos).unwrap();

        let read_start = Instant::now();
        let read_trans = conn.transaction().unwrap();
        let read_sql = "SELECT * FROM manta_bucket WHERE owner = $1 AND bucket = $2 AND key = $3";

        read_trans
            .execute(read_sql, &[&o.owner, &o.bucket, &o.key])
            .unwrap();

        read_trans.commit().unwrap();
        let read_end = Instant::now();

        let read_duration = read_end.duration_since(read_start);
        let read_nanos =
            read_duration.as_secs() * 1_000_000_000 + read_duration.subsec_nanos() as u64;
        read_histogram.increment(read_nanos).unwrap();
    }

    (read_histogram, write_histogram)
}

fn delete_public_table(conn: &Connection) {
    let trans = conn.transaction().unwrap();

    trans.execute("DELETE FROM manta_bucket;", &[]).unwrap();

    trans.commit().unwrap();
}

fn delete_schema_tables(conn: &Connection) {
    for number in 1..10000 {
        let trans = conn.transaction().unwrap();
        let schema_name = "manta_bucket_".to_string() + &number.to_string();
        let delete_sql = "DELETE FROM ".to_string() + &schema_name + &".manta_bucket".to_string();
        trans.execute(delete_sql.as_str(), &[]).unwrap();
        trans.commit().unwrap();
    }
}

fn run_multiple_schema_threads(thread_count: &i32) {
    let mut handles = Vec::new();

    let mut read_histogram = Histogram::new();
    let mut write_histogram = Histogram::new();

    for _number in 1..*thread_count {
        let h = thread::spawn(move || multiple_schema_queries());
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

fn multiple_schema_queries() -> (histogram::Histogram, histogram::Histogram) {
    let args: Vec<String> = env::args().collect();
    let url = &args[1];
    let thread_iterations = args.get(3)
        .unwrap_or(&THREAD_ITERATIONS.to_string())
        .parse()
        .unwrap_or(THREAD_ITERATIONS);

    let schema = rand::thread_rng().gen::<u32>() % 10000;
    let schema_name = "manta_bucket_".to_string() + &schema.to_string();

    let mut read_histogram = Histogram::new();
    let mut write_histogram = Histogram::new();
    let conn = Connection::connect(url.as_str(), TlsMode::None).unwrap();

    let mut rng = thread_rng();

    for _number in 1..thread_iterations {
        let k: String = iter::repeat(())
            .map(|()| rng.sample(Alphanumeric))
            .take(10)
            .collect();
        let b: String = iter::repeat(())
            .map(|()| rng.sample(Alphanumeric))
            .take(12)
            .collect();
        let o = MantaObject {
            key: k,
            bucket: b,
            vnode: 1000,
            owner: Uuid::new_v4(),
            object_id: Uuid::new_v4(),
            content_length: 1024,
            content_md5: "deadbeef".to_string(),
            content_type: "text/plain".to_string(),
        };

        let write_start = Instant::now();
        let write_trans = conn.transaction().unwrap();
        let write_sql = "INSERT INTO ".to_string() + &schema_name + &".manta_bucket (key, bucket, vnode, owner, \"objectId\", \"contentLength\", \"contentMD5\", \"contentType\") VALUES ($1, $2, $3, $4, $5, $6, $7, $8)".to_string();

        write_trans
            .execute(
                write_sql.as_str(),
                &[
                    &o.key,
                    &o.bucket,
                    &o.vnode,
                    &o.owner,
                    &o.object_id,
                    &o.content_length,
                    &o.content_md5,
                    &o.content_type,
                ],
            )
            .unwrap();

        write_trans.commit().unwrap();
        let write_end = Instant::now();

        let write_duration = write_end.duration_since(write_start);
        let write_nanos =
            write_duration.as_secs() * 1_000_000_000 + write_duration.subsec_nanos() as u64;
        write_histogram.increment(write_nanos).unwrap();

        let read_start = Instant::now();
        let read_trans = conn.transaction().unwrap();
        let read_sql = "SELECT * FROM ".to_string()
            + &schema_name
            + &".manta_bucket WHERE owner = $1 AND bucket = $2 AND key = $3".to_string();

        read_trans
            .execute(read_sql.as_str(), &[&o.owner, &o.bucket, &o.key])
            .unwrap();

        read_trans.commit().unwrap();
        let read_end = Instant::now();

        let read_duration = read_end.duration_since(read_start);
        let read_nanos =
            read_duration.as_secs() * 1_000_000_000 + read_duration.subsec_nanos() as u64;
        read_histogram.increment(read_nanos).unwrap();
    }

    (read_histogram, write_histogram)
}

fn usage() {
    println!("Usage: multiple-schema-test PG_URL [THREAD_COUNT][THREAD_ITERATIONS]");
}

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        usage();
        process::exit(1);
    }

    let url = &args[1];
    let thread_count = args.get(2)
        .unwrap_or(&THREAD_COUNT.to_string())
        .parse()
        .unwrap_or(THREAD_COUNT);

    println!("Thread count: {:?}", thread_count);

    let conn = Connection::connect(url.as_str(), TlsMode::None).unwrap();

    let start1 = Instant::now();
    run_single_schema_threads(&thread_count);
    let end1 = Instant::now();
    println!("Single schema duration: {:?}", end1.duration_since(start1));

    delete_public_table(&conn);

    thread::sleep(Duration::from_secs(1));

    let start2 = Instant::now();
    run_multiple_schema_threads(&thread_count);
    let end2 = Instant::now();
    println!(
        "Multiple schema duration: {:?}",
        end2.duration_since(start2)
    );

    delete_schema_tables(&conn);
}
