use std::sync::Arc;

use color_eyre::Result;
use datafusion::arrow::array::{Float64Array, Int32Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::*;
use datafusion_example::utils::dataframe::register_df_view;

/// Round-Robin Selection of Workers
pub async fn round_robin_example() -> Result<()> {
    let ctx = SessionContext::new();
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("data", DataType::Float64, true),
    ]);
    let batch = RecordBatch::try_new(
        schema.clone().into(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
            Arc::new(Float64Array::from(vec![1.0, 10.0, 100.0])),
        ],
    )?;
    let df = ctx.read_batch(batch.clone())?;
    let table_name = "t";
    register_df_view(&ctx, &df, table_name)?;

    let mut cur_worker = 1;
    while cur_worker <= 5 {
        let round_robin_sql =
            format!("(({cur_worker} - 1) % (select count(*) from {table_name})) + 1");
        let df = ctx
            .sql(&format!(
                "select * from {table_name} where id = {round_robin_sql}"
            ))
            .await?;
        df.show().await?;
        cur_worker += 1;
    }

    Ok(())
}
