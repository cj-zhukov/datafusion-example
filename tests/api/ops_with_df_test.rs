use std::sync::Arc;

use datafusion::arrow::array::{Array, Int32Array, RecordBatch, StringArray};
use datafusion::assert_batches_eq;
use datafusion::prelude::*;
use tempfile::tempdir;

use datafusion_example::df;
use datafusion_example::utils::utils::*;

use crate::helpers::{get_df1, get_schema};

#[tokio::test]
async fn test_df_macro() {
    let id = Int32Array::from(vec![1, 2, 3]);
    let data = Int32Array::from(vec![42, 43, 44]);
    let name = StringArray::from(vec![Some("foo"), Some("bar"), None]);

    let df = df!(
        "id" => id,
        "data" => data,
        "name" => name
    );

    assert_eq!(df.schema().fields().len(), 3); // columns count
    assert_eq!(df.clone().count().await.unwrap(), 3); // rows count

    let rows = df.sort(vec![col("id").sort(true, true)]).unwrap();
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
        &rows.collect().await.unwrap()
    );
}

#[tokio::test]
async fn test_df_sql() {
    let df = get_df1().unwrap();
    let sql = r#"id > 2 and data > 43 and name in ('foo', 'bar', 'baz')"#;
    let res = df_sql(df, sql).await.unwrap();

    assert_eq!(res.schema().fields().len(), 3);
    assert_eq!(res.clone().count().await.unwrap(), 1);

    let rows = res.sort(vec![col("id").sort(true, true)]).unwrap();
    assert_batches_eq!(
        &[
            "+----+------+------+",
            "| id | name | data |",
            "+----+------+------+",
            "| 3  | baz  | 44   |",
            "+----+------+------+",
        ],
        &rows.collect().await.unwrap()
    );
}

#[tokio::test]
async fn test_is_empty() {
    let ctx = SessionContext::new();
    let df = get_df1().unwrap();
    assert_eq!(is_empty(df).await.unwrap(), false);

    let schema = get_schema();
    let batch = RecordBatch::new_empty(Arc::new(schema));
    let df = ctx.read_batch(batch).unwrap();
    assert_eq!(is_empty(df).await.unwrap(), true);
}

#[tokio::test]
async fn test_df_to_table() {
    let ctx = SessionContext::new();
    let df = get_df1().unwrap();
    df_to_table(&ctx, df, "t").await.unwrap();
    let res = ctx.sql("select * from t order by id").await.unwrap();
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
        &res.collect().await.unwrap()
    );
}

#[tokio::test]
async fn test_write_df_to_file() {
    let ctx = SessionContext::new();
    let df = get_df1().unwrap();
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("foo.parquet");
    write_df_to_file(df, file_path.to_str().unwrap()).await.unwrap();
    let res = ctx.read_parquet(file_path.to_str().unwrap(), ParquetReadOptions::default()).await.unwrap();
    let rows = res.sort(vec![col("id").sort(true, true)]).unwrap();
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
        &rows.collect().await.unwrap()
    );
}
