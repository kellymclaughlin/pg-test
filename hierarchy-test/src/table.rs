extern crate histogram;
extern crate postgres;
extern crate rand;
extern crate uuid;

use std::sync::Arc;
use std::thread;
use std::time::Instant;

use histogram::Histogram;
use postgres::{Connection, TlsMode};
use rand::{thread_rng, Rng};

use common;
use types::{HistogramPair, MantaObject};


pub fn run_threads(url: Arc<String>,
                   thread_count: &u32,
                   thread_iterations: Arc<u32>,
                   table_count: Arc<u32>) {
    let mut handles = Vec::new();

    let mut read_histogram = Histogram::new();
    let mut write_histogram = Histogram::new();

    for _number in 1..*thread_count {
        let url_clone = Arc::clone(&url);
        let thread_iterations_clone = Arc::clone(&thread_iterations);
        let table_count_clone = Arc::clone(&table_count);
        let h = thread::spawn(|| multiple_database_queries(url_clone,
                                                           thread_iterations_clone,
                                                           table_count_clone));
        handles.push(h);
    }

    for handle in handles {
        match handle.join() {
            Ok((thread_read_hist, thread_write_hist)) => {
                read_histogram.merge(&thread_read_hist);
                write_histogram.merge(&thread_write_hist);
            }
            Err(_) => println!("database thread panicked"),
        }
    }

    common::print_results(&read_histogram, &write_histogram);
}


fn multiple_database_queries(url: Arc<String>,
                             thread_iterations: Arc<u32>,
                             table_count: Arc<u32>) -> HistogramPair {
    let mut rng = thread_rng();
    let table = (rng.gen::<u32>() + 1) % *table_count;
    let table_name = ["manta_bucket_object_", &table.to_string()].concat();

    let mut read_histogram = Histogram::new();
    let mut write_histogram = Histogram::new();
    let conn = Connection::connect(&*url.as_str(), TlsMode::None).unwrap();

    for _number in 1..*thread_iterations {
        let o = MantaObject::new(&mut rng);

        let write_start = Instant::now();
        let write_trans = conn.transaction().unwrap();
        let write_sql = ["INSERT INTO ",
                         &table_name,
                         &" (id, owner, bucket_id, name, vnode, \
                           content_length, content_md5, content_type, headers, sharks) \
                           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)"].concat();

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
        let read_sql = ["SELECT * FROM ",
                        &table_name,
                        &" WHERE owner = $1 AND bucket_id = $2 \
                          AND name = $3"].concat();

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


pub fn delete_tables(conn: &Connection, table_count: u32) {
    for number in 1..table_count {
        let trans = conn.transaction().unwrap();
        let delete_sql = ["DELETE FROM ",
                          &"manta_bucket_object_",
                          &number.to_string()].concat();
        trans.execute(delete_sql.as_str(), &[]).unwrap();
        trans.commit().unwrap();
    }
}
