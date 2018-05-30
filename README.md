# pg-test

A collection of small utilities for examining different aspects of PostgreSQL
behavior and performance.

## Build

* Install rust
* `cargo build --release`

## Utilities

### insert-test

A utility for examining the difference between batching multiple INSERT
statements in a PostgreSQL transaction versus executing the same number of
INSERT statements, but one per transaction.

Usage:

```
insert-test PG_URL [THREAD_COUNT] [THREAD_INSERTS] [BATCH_SIZE]
```

### update-contention-test

A utility for comparing the performance between using a single cell for a
frequently updated tally counter versus spreading the tally updates over a
number of buckets and then summing the buckets on read.

Usage:

```
update-contention-test PG_URL [THREAD_COUNT][THREAD_WRITES] [BUCKET_COUNT]
```
