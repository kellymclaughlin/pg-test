extern crate clap;

use std::str::FromStr;

use clap::{App, Arg, ArgMatches};


pub enum Mode {
    Baseline,
    Database,
    Schema,
    Table
}

impl FromStr for Mode {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "baseline" => Ok(Mode::Baseline),
            "database" => Ok(Mode::Database),
            "schema"   => Ok(Mode::Schema),
            "table"    => Ok(Mode::Table),
            _          => Err("invalid mode")
        }
    }
}

pub fn parse<'a, 'b>(app: String) -> ArgMatches<'a> {
    App::new(app)
        .about("Tool to test different hierarchy options offered by PostgreSQL")
        .version(crate_version!())
        .arg(Arg::with_name("mode")
             .help("Mode of operation")
             .long("mode")
             .short("m")
             .takes_value(true)
             .required(true)
             .possible_values(&["baseline", "database", "schema", "table"]))
        .arg(Arg::with_name("threadIterations")
             .help("Iterations per thread (Default: 1000)")
             .short("i")
             .long("iterations")
             .takes_value(true))
        .arg(Arg::with_name("threadCount")
             .help("Thread count (Default: 16)")
             .short("t")
             .long("thread-count")
             .takes_value(true))
        .arg(Arg::with_name("schemaCount")
             .help("Number of schemas to use for test (Default: 10000)")
             .short("s")
             .long("schema-count")
             .takes_value(true))
        .arg(Arg::with_name("databaseCount")
             .help("Number of databases to use for test (Default: 10000)")
             .short("d")
             .long("database-count")
             .takes_value(true))
        .arg(Arg::with_name("tableCount")
             .help("Number of tables to use for test (Default: 10000)")
             .long("table-count")
             .takes_value(true))
        .arg(Arg::with_name("url")
             .help("Postgres URL")
             .short("u")
             .long("url")
             .takes_value(true)
             .required(true))
        .get_matches()
}
