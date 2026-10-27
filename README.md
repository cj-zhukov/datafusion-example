# datafusion-example

DataFusion tools and examples in Rust.

- **`src/utils`** — helpers for working with `DataFrame`s: convert columns to
  JSON or structs, add columns, concat/join frames, read & write files,
  S3 via `object_store`.
- **`examples/`** — runnable examples grouped by topic (`dataframe`, `algo`,
  `udf`). See [`examples/README.md`](examples/README.md) for the full list.
- **`benches/`** — criterion benchmarks for the hot utils.

## Quick start

```bash
cargo run --example dataframe -- all          # run a whole group
cargo run --example dataframe -- query        # run one example
cargo test
cargo bench
```

Requires Rust 2024 edition (1.85+). DataFusion 55.
