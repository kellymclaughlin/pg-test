# pg-test

A small utility for examining the difference between batching multiple INSERT
statements in a PostgreSQL transaction versus executing the same number of
INSERT statements, but one per transaction.

## Build

* Install rust
* `cargo build --release`

## Usage

```
pg-test PG_URL [THREAD_COUNT] [THREAD_INSERTS] [BATCH_SIZE]
```
