extern crate histogram;
extern crate postgres;
extern crate rand;
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

use common;
use types::{HistogramPair, MantaObject};


pub fn run_threads(url: Arc<String>,
                   thread_count: &u32,
                   thread_iterations: Arc<u32>,
                   schema_count: Arc<u32>) {
    let mut handles = Vec::new();

    let mut read_histogram = Histogram::new();
    let mut write_histogram = Histogram::new();

    for _number in 1..*thread_count {
        let url_clone = Arc::clone(&url);
        let thread_iterations_clone = Arc::clone(&thread_iterations);
        let schema_count_clone = Arc::clone(&schema_count);
        let h = thread::spawn(|| multiple_schema_queries(url_clone,
                                                         thread_iterations_clone,
                                                         schema_count_clone));
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


fn multiple_schema_queries(url: Arc<String>,
                           thread_iterations: Arc<u32>,
                           schema_count: Arc<u32>) -> HistogramPair {
    let schema = rand::thread_rng().gen::<u32>() % *schema_count;
    let schema_name = "manta_bucket_".to_string() + &schema.to_string();

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
            content_md5: String::from("deadbeef"),
            content_type: String::from("text/plain"),
            headers: common::headers(),
            sharks: common::sharks()
        };

        let write_start = Instant::now();
        let write_trans = conn.transaction().unwrap();
        let write_sql = "INSERT INTO ".to_string()
            + &schema_name
            + &".manta_bucket_object (id, owner, bucket_id, name, vnode, \
                content_length, content_md5, content_type, headers, sharks) \
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)".to_string();

        write_trans.execute(write_sql.as_str(),
                            &[
                                &o.id,
                                &o.owner,
                                &o.bucket_id,
                                &o.name,
                                &o.vnode,
                                &o.content_length,
                                &o.content_md5,
                                &o.content_type,
                                &o.headers,
                                &o.sharks
                            ]).unwrap();

        write_trans.commit().unwrap();
        let write_end = Instant::now();

        let write_duration = write_end.duration_since(write_start);
        let write_nanos = write_duration.as_secs() * 1_000_000_000
            + write_duration.subsec_nanos() as u64;
        write_histogram.increment(write_nanos).unwrap();

        let read_start = Instant::now();
        let read_trans = conn.transaction().unwrap();
        let read_sql = "SELECT * FROM ".to_string()
            + &schema_name
            + &".manta_bucket_object WHERE owner = $1 AND bucket_id = $2 \
                AND name = $3".to_string();

        read_trans
            .execute(read_sql.as_str(), &[&o.owner, &o.bucket_id, &o.name])
            .unwrap();

        read_trans.commit().unwrap();
        let read_end = Instant::now();

        let read_duration = read_end.duration_since(read_start);
        let read_nanos = read_duration.as_secs() * 1_000_000_000
            + read_duration.subsec_nanos() as u64;
        read_histogram.increment(read_nanos).unwrap();
    }

    (read_histogram, write_histogram)
}


pub fn delete_tables(conn: &Connection, schema_count: u32) {
    for number in 1..schema_count {
        let trans = conn.transaction().unwrap();
        let schema_name = "manta_bucket_".to_string() + &number.to_string();
        let delete_sql = "DELETE FROM ".to_string()
            + &schema_name
            + &".manta_bucket_object".to_string();
        trans.execute(delete_sql.as_str(), &[]).unwrap();
        trans.commit().unwrap();
    }
}
