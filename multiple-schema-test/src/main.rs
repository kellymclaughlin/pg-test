#[macro_use]
extern crate clap;
extern crate histogram;
extern crate postgres;
extern crate rand;
extern crate uuid;

mod baseline;
mod common;
mod database;
mod opts;
mod schema;
mod table;
mod types;

use std::error::Error;
use std::process;
use std::sync::Arc;
use std::time::Instant;

use postgres::{Connection, TlsMode};

use opts::Mode;

/*
 * This program was written to look at performance of querying across the three
 * different levels of postgres data hierarchy (i.e. databases, schemas, and
 * tables).
 *
 * The create-vnode-schemas.sh script can be used to facilitate creation of
 * the schemas.
 *
 *     ./create-vnode-schemas.sh {1..10000}
 */

static APP: &'static str = "multiple-schema-test";
const THREAD_COUNT: u32 = 16;
const THREAD_ITERATIONS: u32 = 1000;
const DEFAULT_HIERARCHY_COUNT: u32 = 10000;


fn main() {
    let matches = opts::parse(APP.to_string());

    // The url is guaranteed to be present if we make it here
    let url = matches.value_of("url").unwrap().to_string();
    let thread_count = value_t!(matches, "threadCount", u32)
        .unwrap_or(THREAD_COUNT);
    let thread_iterations = value_t!(matches, "threadIterations", u32)
        .unwrap_or(THREAD_ITERATIONS);
    let mode = value_t!(matches, "mode", Mode).unwrap_or_else(|e| e.exit());

    let conn = Connection::connect(url.as_str(), TlsMode::None)
        .unwrap_or_else(|e| {
            eprintln!("Postgres connection error: {}", e.description());
            process::exit(1)
        });

    match mode {
        Mode::Baseline => {
            let start = Instant::now();
            baseline::run_threads(Arc::new(url),
                                  &thread_count,
                                  Arc::new(thread_iterations));
            let end = Instant::now();
            println!("Baseline mode duration: {:?}", end.duration_since(start));
            baseline::delete_table(&conn);
        },
        Mode::Database => {
            let _database_count = value_t!(matches, "databaseCount", u32)
                .unwrap_or(DEFAULT_HIERARCHY_COUNT);
            let start = Instant::now();
            database::run_threads(&thread_count);
            let end = Instant::now();
            println!(
                "Database mode duration: {:?}",
                end.duration_since(start)
            );
        },
        Mode::Schema => {
            let schema_count = value_t!(matches, "schemaCount", u32)
                .unwrap_or(DEFAULT_HIERARCHY_COUNT);
            let start = Instant::now();
            schema::run_threads(Arc::new(url),
                                &thread_count,
                                Arc::new(thread_iterations),
                                Arc::new(schema_count));
            let end = Instant::now();
            println!(
                "Schema mode duration: {:?}",
                end.duration_since(start)
            );

            schema::delete_tables(&conn, schema_count);
        },
        Mode::Table => {
            let _table_count = value_t!(matches, "tableCount", u32)
                .unwrap_or(DEFAULT_HIERARCHY_COUNT);
            let start = Instant::now();
            table::run_threads(&thread_count);
            let end = Instant::now();
            println!(
                "Table mode duration: {:?}",
                end.duration_since(start)
            );
        }
    }
}
