use std::sync::Arc;

use color_eyre::Result;
use datafusion::arrow::array::{Float64Array, Int32Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::*;

use datafusion_example::utils::dataframe::df_to_table;

pub async fn random_example() -> Result<()> {
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
    df_to_table(&ctx, df, table_name).await?;

    let sql = format!(
        "select *
                from {table_name} 
                where 1 = 1 
                order by random()
                limit 1"
    );

    for _ in 0..5 {
        let df = ctx.sql(&sql).await?;
        df.show().await?;
    }

    Ok(())
}
