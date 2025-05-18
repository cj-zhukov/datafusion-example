use std::sync::Arc;

use color_eyre::Result;
use datafusion::arrow::array::RecordBatch;
use datafusion::assert_batches_eq;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::*;
use tempfile::tempdir;

use datafusion_example::df;
use datafusion_example::utils::tools::*;

use crate::helpers::{get_df1, get_schema};

#[tokio::test]
async fn test_df_macro() -> Result<()> {
    let df = df!(
        "id" => vec![1, 2, 3],
        "data" => vec![42, 43, 44],
        "name" => vec![Some("foo"), Some("bar"), None]
    );

    assert_eq!(df.schema().fields().len(), 3); // columns count
    assert_eq!(df.clone().count().await?, 3); // rows count

    let rows = df.sort(vec![col("id").sort(true, true)])?;
    assert_batches_eq!(
        &[
            "+----+------+------+",
            "| id | data | name |",
            "+----+------+------+",
            "| 1  | 42   | foo  |",
            "| 2  | 43   | bar  |",
            "| 3  | 44   |      |",
            "+----+------+------+",
        ],
        &rows.collect().await?
    );

    Ok(())
}

#[tokio::test]
async fn test_df_sql() -> Result<()> {
    let df = get_df1()?;
    let sql = r#"id > 2 and data > 43 and name in ('foo', 'bar', 'baz')"#;
    let res = df_sql(df, sql).await?;

    assert_eq!(res.schema().fields().len(), 3);
    assert_eq!(res.clone().count().await?, 1);

    let rows = res.sort(vec![col("id").sort(true, true)])?;
    assert_batches_eq!(
        &[
            "+----+------+------+",
            "| id | name | data |",
            "+----+------+------+",
            "| 3  | baz  | 44   |",
            "+----+------+------+",
        ],
        &rows.collect().await?
    );

    Ok(())
}

#[tokio::test]
async fn test_is_empty() -> Result<()> {
    let ctx = SessionContext::new();
    let df = get_df1()?;
    assert_eq!(is_empty(df).await?, false);

    let schema = get_schema();
    let batch = RecordBatch::new_empty(Arc::new(schema));
    let df = ctx.read_batch(batch)?;
    assert_eq!(is_empty(df).await?, true);

    Ok(())
}

#[tokio::test]
async fn test_df_to_table() -> Result<()> {
    let ctx = SessionContext::new();
    let df = get_df1()?;
    df_to_table(&ctx, df, "t").await?;
    let res = ctx.sql("select * from t order by id").await?;
    assert_batches_eq!(
        &[
            "+----+------+------+",
            "| id | name | data |",
            "+----+------+------+",
            "| 1  | foo  | 42   |",
            "| 2  | bar  | 43   |",
            "| 3  | baz  | 44   |",
            "+----+------+------+",
        ],
        &res.collect().await?
    );

    Ok(())
}
#[tokio::test]
async fn test_df_plan_to_table() -> Result<()> {
    let ctx = SessionContext::new();
    let df = get_df1()?;
    df_plan_to_table(&ctx, df.logical_plan().clone(), "t").await?;
    let res = ctx.sql("select * from t order by id").await?;
    assert_batches_eq!(
        &[
            "+----+------+------+",
            "| id | name | data |",
            "+----+------+------+",
            "| 1  | foo  | 42   |",
            "| 2  | bar  | 43   |",
            "| 3  | baz  | 44   |",
            "+----+------+------+",
        ],
        &res.collect().await?
    );

    Ok(())
}

#[tokio::test]
async fn test_write_df_to_file() -> Result<()> {
    let ctx = SessionContext::new();
    let df = get_df1()?;
    let dir = tempdir()?;
    let file_path = dir.path().join("foo.parquet");
    write_df_to_file(df, file_path.to_str().unwrap()).await?;
    let res = ctx
        .read_parquet(file_path.to_str().unwrap(), ParquetReadOptions::default())
        .await?;
    let rows = res.sort(vec![col("id").sort(true, true)])?;
    assert_batches_eq!(
        &[
            "+----+------+------+",
            "| id | name | data |",
            "+----+------+------+",
            "| 1  | foo  | 42   |",
            "| 2  | bar  | 43   |",
            "| 3  | baz  | 44   |",
            "+----+------+------+",
        ],
        &rows.collect().await?
    );

    Ok(())
}

#[tokio::test]
async fn test_read_file_to_df() -> Result<()> {
    let ctx = SessionContext::new();
    let df = get_df1()?;
    let dir = tempdir()?;
    let file_path = dir.path().join("foo.parquet");
    df.write_parquet(
        file_path.to_str().unwrap(),
        DataFrameWriteOptions::new(),
        None,
    )
    .await?;
    let res = read_file_to_df(&ctx, file_path.to_str().unwrap()).await?;
    let rows = res.sort(vec![col("id").sort(true, true)])?;
    assert_batches_eq!(
        &[
            "+----+------+------+",
            "| id | name | data |",
            "+----+------+------+",
            "| 1  | foo  | 42   |",
            "| 2  | bar  | 43   |",
            "| 3  | baz  | 44   |",
            "+----+------+------+",
        ],
        &rows.collect().await?
    );

    Ok(())
}
