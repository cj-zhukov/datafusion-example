use std::hint::black_box;

use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::prelude::*;
use tokio::runtime::Runtime;

use datafusion_example::utils::dataframe::df_cols_to_struct;

fn bench_this(c: &mut Criterion) {
    let ctx = SessionContext::new();
    let df = black_box(dataframe!(
        "id" => [1, 2, 3],
        "name" => ["foo", "bar", "baz"],
        "data" => [42, 43, 44]
    )).unwrap();
    let cols = black_box(&["id", "name", "data"]);

    c.bench_function("df_cols_to_json", |b| {
        b.to_async(Runtime::new().unwrap())
            .iter(|| df_cols_to_struct(&ctx, df.clone(), cols, "new_col"))
    });
}

criterion_group!(benches, bench_this);
criterion_main!(benches);
