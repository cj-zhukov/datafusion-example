use std::{hint::black_box, sync::Arc};

use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::{
    arrow::array::{ArrayRef, StringArray},
    prelude::*,
};
use tokio::runtime::Runtime;

use datafusion_example::utils::dataframe::add_column_to_df;

fn bench_this(c: &mut Criterion) {
    let ctx = SessionContext::new();
    let df = black_box(dataframe!(
        "id" => [1, 2, 3],
        "data" => [42, 43, 44]
    ))
    .unwrap();
    let col: ArrayRef = Arc::new(StringArray::from(vec!["foo", "bar", "baz"]));

    c.bench_function("df_cols_to_json", |b| {
        b.to_async(Runtime::new().unwrap())
            .iter(|| add_column_to_df(&ctx, df.clone(), col.clone(), "new_col"))
    });
}

criterion_group!(benches, bench_this);
criterion_main!(benches);
