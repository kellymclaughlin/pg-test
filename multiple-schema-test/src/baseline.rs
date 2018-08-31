extern crate histogram;
extern crate postgres;
extern crate uuid;

use std::iter;
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use histogram::Histogram;
use postgres::{Connection, TlsMode};
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use uuid::Uuid;

use types::{HistogramPair, MantaObject};


pub fn run_threads(url: Arc<String>, thread_count: &u32, thread_iterations: Arc<u32>) {
    let mut handles = Vec::new();
    for _number in 1..*thread_count {
        let url_clone = Arc::clone(&url);
        let thread_iterations_clone = Arc::clone(&thread_iterations);
        let h = thread::spawn(|| single_schema_queries(url_clone, thread_iterations_clone));
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


fn single_schema_queries(url: Arc<String>, thread_iterations: Arc<u32>) -> HistogramPair {
    let mut read_histogram = Histogram::new();
    let mut write_histogram = Histogram::new();
    let conn = Connection::connect(&*url.as_str(), TlsMode::None).unwrap();

    let mut rng = thread_rng();

    for _number in 1..*thread_iterations {
        let name: String = iter::repeat(())
            .map(|()| rng.sample(Alphanumeric))
            .take(10)
            .collect();
        let o = MantaObject {
            name: name,
            id: Uuid::new_v4(),
            bucket_id: Uuid::new_v4(),
            vnode: 1000,
            owner: Uuid::new_v4(),
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
                    &o.name,
                    &o.bucket_id,
                    &o.vnode,
                    &o.owner,
                    &o.id,
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
            .execute(read_sql, &[&o.owner, &o.bucket_id, &o.name])
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


pub fn delete_table(conn: &Connection) {
    let trans = conn.transaction().unwrap();

    trans.execute("DELETE FROM manta_bucket;", &[]).unwrap();

    trans.commit().unwrap();
}
