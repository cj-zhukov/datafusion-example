use std::sync::Arc;

use color_eyre::Result;
use datafusion::arrow::array::{Int32Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    query1().await?;
    query2().await?;
    query3().await?;
    query4().await?;
    view_example().await?;
    cte_example().await?;
    Ok(())
}

pub async fn query1() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 10, 100])),
            Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
        ],
    )?;

    let ctx = SessionContext::new();
    // how to register RecordBatch as table
    ctx.register_batch("t", batch)?;

    let df = ctx
        .sql(
            "select * from t \
            where id > 10",
        )
        .await?;

    df.show().await?;

    Ok(())
}

pub async fn query2() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("data", DataType::Int32, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
            Arc::new(Int32Array::from(vec![42, 43, 44])),
        ],
    )?;

    let ctx = SessionContext::new();
    let df = ctx.read_batch(batch)?;

    let sql = r#"id >= 2 and data >= 42 and name in ('foo', 'bar')"#;
    let filter = df.parse_sql_expr(sql)?;
    let res = df.filter(filter)?;

    res.show().await?;

    Ok(())
}

pub async fn query3() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("data", DataType::Int32, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
            Arc::new(Int32Array::from(vec![42, 43, 44])),
        ],
    )?;

    let ctx = SessionContext::new();
    let df = ctx.read_batch(batch)?;
    // how to register Vec<RecordBatch> as table
    let schema = df.clone().schema().as_arrow().clone();
    let batches = df.collect().await?;
    let mem_table = MemTable::try_new(Arc::new(schema), vec![batches])?;
    ctx.register_table("t", Arc::new(mem_table))?;
    let sql = r#"select * from t
                    where id >= 2 
                    and data >= 42 
                    and name in ('foo', 'bar')"#;
    let res = ctx.sql(sql).await?;
    res.show().await?;

    Ok(())
}

pub async fn query4() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_parquet(
        "t",
        ".data/alltypes_plain.parquet",
        ParquetReadOptions::default(),
    )
    .await?;
    let res = ctx.sql("select * from t").await?;
    res.show().await?;
    Ok(())
}

pub async fn view_example() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("data", DataType::Int32, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
            Arc::new(Int32Array::from(vec![42, 43, 44])),
        ],
    )?;
    let ctx = SessionContext::new();
    let df = ctx.read_batch(batch)?;

    let view = df.into_view();
    ctx.register_table("view", view)?;
    let res = ctx.sql("select * from view limit 1").await?;
    res.show().await?;
    Ok(())
}

pub async fn cte_example() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("data", DataType::Int32, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
            Arc::new(Int32Array::from(vec![42, 43, 44])),
        ],
    )?;
    let ctx = SessionContext::new();
    ctx.register_batch("t", batch)?;
    let res = ctx
        .sql("with tmp as (select * from t where data > 42) select count(*) from tmp")
        .await?;
    res.show().await?;
    Ok(())
}
